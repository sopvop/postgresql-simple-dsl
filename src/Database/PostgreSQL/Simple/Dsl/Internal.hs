{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE TypeSynonymInstances       #-}

module Database.PostgreSQL.Simple.Dsl.Internal
       where

import           Control.Applicative
import           Control.Monad                        (ap, liftM)
import           Control.Monad.State.Class
import           Control.Monad.Trans
import           Control.Monad.Trans.State            (State, StateT, runState,
                                                       runStateT)
import           Control.Monad.Trans.Writer
import           Data.ByteString                      (ByteString)
import qualified Data.ByteString.Char8                as B
import           Data.DList                           (DList)
import qualified Data.DList                           as D
import           Data.List                            (intersperse)
import           Data.Monoid
import           Data.Proxy                           (Proxy (..))


import           Blaze.ByteString.Builder             (Builder)
import           Blaze.ByteString.Builder.ByteString  as B
import           Blaze.ByteString.Builder.Char8       as B

import           GHC.TypeLits                         (Sing, SingI, Symbol,
                                                       fromSing, sing)

import           Database.PostgreSQL.Simple           ((:.) (..), Only (..))
import           Database.PostgreSQL.Simple.FromField hiding (Field)
import           Database.PostgreSQL.Simple.FromRow
import           Database.PostgreSQL.Simple.ToField
import           Database.PostgreSQL.Simple.ToRow

-- | Wrapper for selecting whole record
newtype Whole a = Whole { getWhole :: a } deriving (Eq, Ord, Show)

instance Record a => FromRow (Whole a) where
  fromRow = fmap Whole . recordRowParser $ recordParser (Proxy :: Proxy a)

-- | Parser for entities with columns
data RecordParser v a = RecordParser
  { recordColumns   :: [ByteString]
  , recordRowParser :: RowParser a
  }

instance Functor (RecordParser v) where
  fmap f (RecordParser cs rp) = RecordParser cs (fmap f rp)
  {-# INLINE fmap #-}

instance Applicative (RecordParser v) where
  pure a = RecordParser mempty (pure a)
  RecordParser cs f <*> RecordParser cs' a = RecordParser (cs `mappend` cs') (f <*> a)
  {-# INLINE pure #-}
  {-# INLINE (<*>) #-}

-- | Class for entities which have columns
class Record v where
  data Field v (t :: Symbol) b :: *
  recordParser :: Proxy v -> RecordParser v v

-- | Class for naming tables
class Record v =>Table v where
  tableName :: Proxy v -> ByteString

fieldSym :: SingI t => Field v t a -> Sing (t::Symbol)
fieldSym _ = sing

fieldColumn :: SingI t => Field v t a -> ByteString
fieldColumn f = B.pack . fromSing $ fieldSym f

-- | Parse named field
takeField :: (SingI f, FromField a) => (Field v f a) -> RecordParser v a
takeField f = RecordParser ([fieldColumn f]) field

type ExprBuilder = DList Action

-- | Adds phantom type to RawExpr
newtype Expr a = Expr { getRawExpr :: ExprBuilder }

instance ToField (Expr a) where
  toField (Expr a) = Many $ D.toList a


plain :: ByteString -> Action
plain = Plain . B.fromByteString

addParens :: ExprBuilder -> ExprBuilder
addParens t = raw "(" <> t <> raw ")"

opt :: Monoid a => Maybe a -> a
opt Nothing = mempty
opt (Just x) = x

fprepend :: (Functor f, Monoid b) => b -> f b -> f b
fprepend p a = (p <>) <$> a
fappend :: (Functor f, Monoid b) => f b -> b -> f b
fappend a x = (<> x) <$> a

raw :: ByteString -> ExprBuilder
raw = D.singleton . plain

rawField :: ToField a => a -> ExprBuilder
rawField = D.singleton . toField

binOp :: Action -> ExprBuilder -> ExprBuilder -> ExprBuilder
binOp op a b = addParens a <> D.singleton op <> addParens b

binOpE :: Action -> ExprBuilder -> ExprBuilder -> Expr a
binOpE op a b = Expr (binOp op a b)

mkAnd :: ExprBuilder -> ExprBuilder -> ExprBuilder
mkAnd a b = binOp (plain " AND ") a b

mkAccess :: ExprBuilder -> ExprBuilder -> DList Action
mkAccess a b = addParens a <> raw "." <> addParens b

commaSep :: [ExprBuilder] -> ExprBuilder
commaSep = D.concat . intersperse (D.singleton . Plain $ B.fromChar ',')

data Rel r = Rel ExprBuilder
           | RelAliased ExprBuilder

-- | Source of uniques
newtype NameSource = NameSource { getNameSource :: Int }

-- | Select query
mkRename :: ExprBuilder -> ExprBuilder -> ExprBuilder
mkRename s nm = s <> raw " AS " <> nm

class IsExpr a where
  type FromExpr a :: *
  compileProjection :: MonadState NameSource m => a -> m ([ExprBuilder], a)
  asValues  :: a -> [ExprBuilder]
  asRenamed :: MonadState NameSource m => a -> m a

instance Record a => IsExpr (Rel a) where
  type FromExpr (Rel a) = Whole a
  compileProjection rel = do
    nm <- grabName
    let projector c = case rel of
         Rel r -> r <> raw ".\"" <> raw c <> raw "\" AS " <> renamed nm c
         RelAliased r -> raw "\""<> renamed r c <> raw "\""
        proj = map projector columns
    return (proj, RelAliased nm )
    where
      renamed nm c = nm <> raw "__" <> raw c
      columns = recordColumns $ recordParser (Proxy :: Proxy a)

  asRenamed _ = do
    nm <- grabName
    return $ RelAliased nm

  asValues rel = map projector columns
     where
       projector c = case rel of
         Rel r -> r <> raw ".\"" <> raw c <> raw "\""
         RelAliased r -> raw "\""<> renamed r c <> raw "\""
       renamed nm c = nm <> raw "__" <> raw c
       columns = recordColumns $ recordParser (Proxy :: Proxy a)

renameAs :: [ExprBuilder] -> [ExprBuilder] -> [ExprBuilder]
renameAs srs dsts = [x <> raw " AS " <> y | (x,y) <- zip srs dsts]
renameIt :: forall t m . (MonadState NameSource m, IsExpr t) => t -> m ([ExprBuilder], t)
renameIt a = do
  ren <- asRenamed a
  return (renameAs (asValues a) (asValues ren), a)

instance IsExpr (Expr (Only a)) where
  type FromExpr (Expr (Only a)) = Only a
  compileProjection = renameIt
  asValues (Expr a) = [a]
  asRenamed _ = liftM Expr grabName

instance IsExpr (Expr a, Expr b) where
  type FromExpr (Expr a, Expr b) = (a, b)
  compileProjection = renameIt
  asValues (Expr a, Expr b) = [a,b]
  asRenamed _ = return (,) `ap` grabExpr `ap` grabExpr

instance IsExpr (Expr a, Expr b, Expr c) where
  type FromExpr (Expr a, Expr b, Expr c) = (a, b, c)
  compileProjection = renameIt
  asValues (Expr a, Expr b, Expr c) = [a,b,c]
  asRenamed _ = return (,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr

instance IsExpr (Expr a, Expr b, Expr c, Expr d) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d) = (a, b, c, d)
  compileProjection = renameIt
  asValues (Expr a, Expr b, Expr c, Expr d) = [a,b,c,d]
  asRenamed _ = return (,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr

instance IsExpr (Expr a, Expr b, Expr c, Expr d, Expr e) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e) = (a, b, c, d, e)
  compileProjection = renameIt
  asValues (Expr a, Expr b, Expr c, Expr d, Expr e) = [a,b,c,d,e]
  asRenamed _ = return (,,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr
                `ap` grabExpr

instance IsExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = (a, b, c, d, e, f)
  compileProjection = renameIt
  asValues (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = [a,b,c,d,e,f]
  asRenamed _ = return (,,,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr
                `ap` grabExpr `ap` grabExpr

instance IsExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) = (a, b, c, d, e, f, g)
  compileProjection = renameIt
  asValues (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) = [a,b,c,d,e,f,g]
  asRenamed _ = return (,,,,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr
                `ap` grabExpr `ap` grabExpr `ap` grabExpr

instance (IsExpr a, IsExpr b) => IsExpr (a:.b) where
  type FromExpr (a :. b) = (FromExpr a :. FromExpr b)
  compileProjection (a :. b) = do
    (pa, ea) <- compileProjection a
    (pb, eb) <- compileProjection b
    return (pa ++ pb, ea :. eb)
  asValues (a:.b) = asValues a ++ asValues b
  asRenamed ~(a:.b) = return (:.) `ap` asRenamed a `ap` asRenamed b

class ToRow a => ToExpr a where
  type AsExpr a :: *
  toExpr :: a -> AsExpr a

instance ToField a => ToExpr (Only a) where
  type AsExpr (Only a) = Expr (Only a)
  toExpr (Only a) = Expr $ rawField a
instance (ToField a, ToField b) => ToExpr (a,b) where
  type AsExpr (a,b) = (Expr a, Expr b)
  toExpr (a,b) = (Expr $ rawField a, Expr $ rawField b)

instance (ToField a, ToField b, ToField c) => ToExpr (a,b,c) where
  type AsExpr (a,b,c) = (Expr a, Expr b, Expr c)
  toExpr (a,b,c) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c)

instance (ToField a, ToField b, ToField c, ToField d) => ToExpr (a,b,c,d) where
  type AsExpr (a,b,c,d) = (Expr a, Expr b, Expr c, Expr d)
  toExpr (a,b,c,d) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                      Expr $ rawField d)

instance (ToField a, ToField b, ToField c, ToField d, ToField e) => ToExpr (a,b,c,d,e) where
  type AsExpr (a,b,c,d,e) = (Expr a, Expr b, Expr c, Expr d, Expr e)
  toExpr (a,b,c,d,e) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                       Expr $ rawField d, Expr $ rawField e)

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f)
         => ToExpr (a,b,c,d,e,f) where
  type AsExpr (a,b,c,d,e,f) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  toExpr (a,b,c,d,e,f) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                          Expr $ rawField d, Expr $ rawField e, Expr $ rawField f)

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f, ToField g)
         => ToExpr (a,b,c,d,e,f,g) where
  type AsExpr (a,b,c,d,e,f,g) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
  toExpr (a,b,c,d,e,f,g) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                            Expr $ rawField d, Expr $ rawField e, Expr $ rawField f,
                            Expr $ rawField g)

instance (ToExpr a, ToExpr b) => ToExpr (a:.b) where
  type AsExpr (a:.b) = AsExpr a :. AsExpr b
  toExpr (a:.b) = toExpr a :. toExpr b

{-
data UpdatingAct = DoUpdate [(ByteString, ExprBuilder)]
                 | DoInsert [(ByteString, ExprBuilder)]
                 | DoInsertMany [ByteString] [ExprBuilder]
                 | DoDelete

data Updating a = Updating
  { updatingAct    :: UpdatingAct
  , updatingTable  :: ExprBuilder
  }
-}
--newtype Update a = Update { runUpdate :: State NameSource (Updating a) }
{-
compileUpdateAct :: IsExpr a => Selector a -> [(ByteString, RawExpr)] -> ExprBuilder
              -> Namer (ExprBuilder, ExprBuilder, AsExpr a)
compileUpdateAct sel upd tbl = do
  nm <- grabName
  (with, from, where', project, access) <- prepareSelector nm sel
  let setClause = commaSep $ map mkUpd upd
      updTable = raw " UPDATE " <> tbl <> raw " SET "
      ret = case D.toList project of
        [] -> mempty
        xs -> D.fromList (plain " RETURNING " : xs)
      from' :: Maybe (ExprBuilder)
      from' = raw " FROM " `fprepend` from
  return $ (with <> updTable <> setClause <> opt from' <> where', ret, access)
  where
    mkUpd (bs, act) = D.singleton (Plain $ B.fromByteString bs <> B.fromByteString "=")
                          <> act

compileInsertAct :: IsExpr a => Selector a -> [(ByteString, RawExpr)] -> ExprBuilder
              -> Namer (ExprBuilder, ExprBuilder, AsExpr a)
compileInsertAct sel upd tbl = do
  nm <- grabName
  (with, from, where', project, access) <- prepareSelector nm sel
  let cols = commaSep $ map (D.singleton.plain.fst) upd
      vals = commaSep $ map (snd) upd
      updTable = raw " INSERT INTO " <> tbl <> raw "("
               <> cols <> raw ") SELECT " <> vals
      ret = case D.toList project of
        [] -> mempty
        xs -> D.fromList (plain " RETURNING " : xs)
      from' = raw " FROM " `fprepend` from
  return $ (with <> updTable <> opt from' <> where', ret, access)

compileDeleteAct :: IsExpr a => Selector a -> ExprBuilder
              -> Namer (ExprBuilder, ExprBuilder, AsExpr a)
compileDeleteAct sel tbl = do
  nm <- grabName
  (with, from, where', project, access) <- prepareSelector nm sel
  let updTable = raw " DELETE FROM " <> tbl
      ret = case D.toList project of
        [] -> mempty
        xs -> D.fromList (plain " RETURNING " : xs)
      from' = raw " USING " `fprepend` from
  return $ (with <> updTable <> opt from' <> where', ret, access)


compileUpdate :: IsExpr a => Update a -> Namer (ExprBuilder, ExprBuilder, AsExpr a)
compileUpdate (Update mupd) = do
  upd <- mupd
  case updatingAct upd of
     DoUpdate ups -> compileUpdateAct (updatingSelect upd) ups (updatingTable upd)
     DoInsert ups -> compileInsertAct (updatingSelect upd) ups (updatingTable upd)
     DoDelete -> compileDeleteAct (updatingSelect upd) (updatingTable upd)

finishUpdate :: (IsExpr a) => Update a -> Action
finishUpdate q = Many . D.toList $ upd <> ret
  where (upd, ret, _) = fst $  runState (compileUpdate q) (NameSource 0)

finishUpdateNoRet q = Many . D.toList $ upd
  where (upd, _, _) = fst $  runState (compileUpdate q) (NameSource 0)



-}

data QueryState = QueryState
  { queryStateWith      :: Maybe ExprBuilder
  , queryStateRecursive :: Any
  , queryStateFrom      :: Maybe ExprBuilder
  , queryStateWhere     :: Maybe ExprBuilder
  , queryStateOrder     :: Maybe ExprBuilder
  , queryStateLimit     :: Maybe Int
  , queryStateOffset    :: Maybe Int
  }

instance Monoid QueryState where
  mempty = QueryState Nothing mempty Nothing Nothing Nothing Nothing Nothing
  QueryState wa ra fa wha sa la oa `mappend` QueryState wb rb fb whb sb lb ob =
        QueryState (joinWith (raw ",") wa wb)
                   (ra <> rb)
                   (joinWith (raw ",") fa fb)
                   (joinWith (raw " AND ") wha whb)
                   (joinWith (raw ",") sa sb)
                   (getLast (Last la <> Last lb))
                   (getLast (Last oa <> Last ob))

joinWith :: Monoid a => a -> Maybe a -> Maybe a -> Maybe a
joinWith j (Just a) (Just b) = Just (a <> j <> b)
joinWith _ (Just a) _ = Just a
joinWith _ _ (Just b) = Just b
joinWith _ _ _ = Nothing

type QueryM a = StateT NameSource (Writer QueryState) a
newtype Query a = Query { runQuery :: StateT NameSource (Writer QueryState) a }
        deriving (Functor, Monad, Applicative)

grabName_ :: MonadState NameSource m => m Builder
grabName_ = do
  NameSource num <- get
  put (NameSource $ succ num)
  return $ B.fromChar 'q' <> B.fromShow num

grabName :: MonadState NameSource m => m (ExprBuilder)
grabName = liftM (D.singleton . Plain) grabName_

grabExpr :: MonadState NameSource m => m (Expr a)
grabExpr = liftM Expr grabName

compIt :: MonadState NameSource m => Query t -> m (t, QueryState)
compIt (Query a) = do
  ns <- get
  let ((r, ns'), q) = runWriter (runStateT a ns)
  put ns'
  return (r,q)

finishIt :: [ExprBuilder] -> QueryState -> ExprBuilder
finishIt expr q =
        opt (raw withStart `fprepend` queryStateWith q)
        <> raw "SELECT " <> commaSep expr
        <> opt (raw " FROM "`fprepend` queryStateFrom q)
        <> opt (raw " WHERE " `fprepend` queryStateWhere q)
        <> opt (raw " ORDER BY " `fprepend` queryStateOrder q)
        <> opt (mappend (raw " LIMIT ") . rawField <$> queryStateLimit q)
        <> opt (mappend (raw " OFFSET ") .rawField <$> queryStateOffset q)
  where
    withStart = if getAny (queryStateRecursive q) then " WITH RECURSIVE "
                 else " WITH "
compileQuery :: Query [ExprBuilder] -> QueryM ExprBuilder
compileQuery q = do
  (r, q') <- compIt q
  return $ finishIt r q'

finishQuery :: IsExpr a => Query a -> Action
finishQuery mq = Many $ D.toList res
  where
    res = fst $ runState finisher (NameSource 0)
    finisher = do
      (r, q') <- compIt mq
      (proj, _) <- compileProjection r
      return $ finishIt proj q'

finishQueryNoRet :: Query t -> Action
finishQueryNoRet mq = Many $ D.toList res
  where
    res = fst $ runState finisher (NameSource 0)
    finisher = do
      (_, q') <- compIt mq
      return $ finishIt [raw "true"] q'

whereQ :: Expr t -> Query ()
whereQ (Expr r) = Query $ do
  lift $ tell mempty {queryStateWhere = Just r}

type FromM a = State NameSource a


data FromD a where
   FromTable      :: ExprBuilder -> ExprBuilder -> a -> FromD a
   FromValues     :: ExprBuilder -> ExprBuilder -> ExprBuilder -> a -> FromD a
   FromInnerJoin  :: FromD a -> FromD b -> ExprBuilder -> FromD (a:.b)
   FromCrossJoin  :: FromD a -> FromD b -> FromD (a:.b)


fromExpr :: FromD a -> a
fromExpr (FromTable _ _ a) = a
fromExpr (FromValues _ _ _ a) = a
fromExpr (FromInnerJoin a b _) = fromExpr a :. fromExpr b
fromExpr (FromCrossJoin a b) = fromExpr a :. fromExpr b

newtype From a = From { runFrom :: State NameSource (FromD a) }


compileFrom :: FromD a -> (ExprBuilder, a)
compileFrom (FromTable bs alias a) = (bs<> raw " AS " <> alias, a)
compileFrom (FromValues bs alias proj a) = (raw "(VALUES "<> bs <> raw ") AS "
            <> alias <> raw "(" <> proj <> raw ")" , a)
compileFrom (FromInnerJoin a b cond) = (bld, ea:.eb)
  where
    (ca, ea) = compileFrom a
    (cb, eb) = compileFrom b
    bld = ca <> raw " INNER JOIN " <> cb <> raw " ON " <> cond

compileFrom (FromCrossJoin a b) = (ca <> raw " CROSS JOIN " <> cb, ea:.eb)
  where
    (ca, ea) = compileFrom a
    (cb, eb) = compileFrom b

newtype Sorting = Sorting [ExprBuilder]
  deriving (Monoid)

compileSorting :: Sorting -> ExprBuilder
compileSorting (Sorting r) = mconcat $ intersperse (raw ",") r


newtype UpdExpr a = UpdExpr { getUpdates :: [(ByteString, ExprBuilder)] }
instance Monoid (UpdExpr a) where
  mempty = UpdExpr mempty
  UpdExpr a `mappend` UpdExpr b = UpdExpr (a<>b)

data InsertWriter = InsertWriter
  { insertWriterFrom :: QueryState
  , insertWriterSets :: [(ByteString, ExprBuilder)]
  }

instance Monoid InsertWriter where
  mempty = InsertWriter mempty mempty
  InsertWriter qa sa `mappend` InsertWriter qb sb =
    InsertWriter (qa <> qb) (sa <> sb)

newtype Inserting r a = Inserting {
        runInserting :: StateT NameSource (Writer InsertWriter) a
    } deriving (Monad, Functor, Applicative)


compileInserting :: MonadState NameSource m => ExprBuilder -> Inserting t r -> m (r, ExprBuilder)
compileInserting table (Inserting a) = do
  ns <- get
  let ((r, ns'), q) = runWriter (runStateT a ns)
  put ns'
  let (cols, exprs) = unzip $ insertWriterSets q
      compiledSel = finishIt exprs $ insertWriterFrom q
      res = raw "INSERT INTO "<>table <> raw "(" <> commaSep (map raw cols)
          <> raw ") (" <> compiledSel <> raw ")"
  return (r,res)

newtype Updating r a = Updating {
        runUpdating :: StateT NameSource (Writer InsertWriter) a
    } deriving (Monad, Functor, Applicative)


finishUpdating :: [(ByteString, ExprBuilder)] -> ExprBuilder -> QueryState -> ExprBuilder
finishUpdating setters table q =
        opt (raw " WITH " `fprepend` queryStateWith q)
        <> raw "UPDATE "<> table
        <> raw " SET " <> sets
        <> opt (raw " FROM "`fprepend` queryStateFrom q)
        <> opt (raw " WHERE " `fprepend` queryStateWhere q)
  where
    (columns, exprs) = unzip setters
    sets = raw "(" <> commaSep (map raw columns) <> raw ")=(" <> commaSep exprs <> raw ")"

compileUpdating :: (MonadState NameSource m) =>
              ExprBuilder -> Updating t r -> m (r, ExprBuilder)
compileUpdating table (Updating a) = do
  ns <- get
  let ((r, ns'), q) = runWriter (runStateT a ns)
  put ns'
  let compiled = finishUpdating (insertWriterSets q) table $ insertWriterFrom q
  return (r,compiled)

newtype Deleting t a = Deleting { runDeleting :: QueryM a}
        deriving (Monad, Functor, Applicative)

compileDeleting :: MonadState NameSource m => ExprBuilder -> Deleting t a -> m (a, ExprBuilder)
compileDeleting table (Deleting x) = do
  (expr, q) <- compIt (Query x)
  let res = opt (raw " WITH " `fprepend` queryStateWith q)
        <> raw "DELETE FROM "<> table
        <> opt (raw " USING "`fprepend` queryStateFrom q)
        <> opt (raw " WHERE " `fprepend` queryStateWhere q)
  return (expr, res)

newtype Update a = Update { runUpdate :: State NameSource (a, ExprBuilder) }

finishUpdate :: IsExpr t => Update t -> Action
finishUpdate (Update u) = Many . D.toList . fst $ runState (u >>= compiler) (NameSource 0)
  where
    compiler (expr, bld) = do
      (proj, _) <- compileProjection expr
      return $ (bld <> raw " RETURNING "<> commaSep proj)

finishUpdateNoRet :: Update a -> Action
finishUpdateNoRet (Update u) = Many . D.toList . snd . fst $ runState u (NameSource 0)

data Function a = Function { functionName :: ByteString
                           , functionArgs :: [ExprBuilder]
                           }

arg :: Expr t -> Function a -> Function a
arg (Expr v) f = f { functionArgs = functionArgs f ++ [v]}

function :: ByteString -> Function a
function bs = Function bs []

call :: Function a -> Expr a
call (Function bs args) = Expr  $
   raw bs <> raw "(" <> commaSep args <> raw ")"

data Union = UnionDistinct | UnionAll deriving (Eq, Show, Ord)

data Recursive a = Recursive Union (Query a) (a -> Query a)
