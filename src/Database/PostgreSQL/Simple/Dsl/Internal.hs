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
import           Control.Monad                        (liftM)
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

-- | Class for naming tables
class Table v where
  tableName :: Proxy v -> ByteString

-- | Class for entities which have columns
class Record v where
  data Field v (t :: Symbol) b :: *
  recordParser :: Proxy v -> RecordParser v v

instance (Record a, Record b) => Record (a,b) where
   recordParser _ = RecordParser (ca <> cb) ((,) <$> rpa <*> rpb)
     where
       RecordParser ca rpa = recordParser (Proxy :: Proxy a)
       RecordParser cb rpb = recordParser (Proxy :: Proxy b)

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

addParens t = raw "(" <> t <> raw ")"

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
  compileProjection :: MonadState NameSource m => a -> m (ExprBuilder, a)

instance Record a => IsExpr (Rel a) where
  type FromExpr (Rel a) = Whole a
  compileProjection r = do
    nm <- grabName
    let projector c = case r of
         Rel r -> r <> raw ".\"" <> raw c <> raw "\" AS " <> renamed nm c
         RelAliased r -> raw "\""<> renamed r c <> raw "\""
        proj = mconcat $ intersperse (raw ",") $ map projector columns
    return (proj, RelAliased nm)
    where
      renamed nm c = nm <> raw "__" <> raw c
      columns = recordColumns $ recordParser (Proxy :: Proxy a)

instance IsExpr (Expr (Only a)) where
  type FromExpr (Expr (Only a)) = Only a
  compileProjection (Expr a) = do
    nm <- grabName
    return (mkRename a nm, Expr nm)


instance IsExpr (Expr a, Expr b) where
  type FromExpr (Expr a, Expr b) = (a, b)
  compileProjection (Expr a, Expr b) = do
    nm1 <- grabName
    nm2 <- grabName
    return $ (mkRename a nm1 <> raw "," <> mkRename b nm2,
             (Expr nm1, Expr nm2))

instance IsExpr (Expr a, Expr b, Expr c) where
  type FromExpr (Expr a, Expr b, Expr c) = (a, b, c)
  compileProjection (Expr a, Expr b, Expr c) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    return $ (  mkRename a nm1 <> raw ","
             <> mkRename b nm2 <> raw ","
             <> mkRename c nm3,
             (Expr nm1, Expr nm2, Expr nm3))

instance IsExpr (Expr a, Expr b, Expr c, Expr d) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d) = (a, b, c, d)
  compileProjection (Expr a, Expr b, Expr c, Expr d) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    return $ (  mkRename a nm1 <> raw ","
             <> mkRename b nm2 <> raw ","
             <> mkRename c nm3 <> raw ","
             <> mkRename d nm4,
             (Expr nm1, Expr nm2, Expr nm3, Expr nm4))

instance IsExpr (Expr a, Expr b, Expr c, Expr d, Expr e) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e) = (a, b, c, d, e)
  compileProjection (Expr a, Expr b, Expr c, Expr d, Expr e) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    nm5 <- grabName
    return $ (  mkRename a nm1 <> raw ","
             <> mkRename b nm2 <> raw ","
             <> mkRename c nm3 <> raw ","
             <> mkRename d nm4 <> raw ","
             <> mkRename e nm5,
             (Expr nm1, Expr nm2, Expr nm3, Expr nm4, Expr nm5))

instance IsExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = (a, b, c, d, e, f)
  compileProjection (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    nm5 <- grabName
    nm6 <- grabName
    return $ (  mkRename a nm1 <> raw ","
             <> mkRename b nm2 <> raw ","
             <> mkRename c nm3 <> raw ","
             <> mkRename d nm4 <> raw ","
             <> mkRename e nm5 <> raw ","
             <> mkRename f nm6,
             (Expr nm1, Expr nm2, Expr nm3, Expr nm4, Expr nm5, Expr nm6))

instance IsExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) where
  type FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) = (a, b, c, d, e, f, g)
  compileProjection (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    nm5 <- grabName
    nm6 <- grabName
    nm7 <- grabName
    return $ (  mkRename a nm1 <> raw ","
             <> mkRename b nm2 <> raw ","
             <> mkRename c nm3 <> raw ","
             <> mkRename d nm4 <> raw ","
             <> mkRename e nm5 <> raw ","
             <> mkRename f nm6 <> raw ","
             <> mkRename g nm7,
             (Expr nm1, Expr nm2, Expr nm3, Expr nm4, Expr nm5, Expr nm6, Expr nm7))


instance (IsExpr a, IsExpr b) => IsExpr (a:.b) where
  type FromExpr (a :. b) = (FromExpr a :. FromExpr b)
  compileProjection (a :. b) = do
    (pa, ea) <- compileProjection a
    (pb, eb) <- compileProjection b
    return (pa <> raw "," <> pb, ea :. eb)


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


data Function a = Function { functionName :: ByteString
                           , functionArgs :: [ExprBuilder]
                           }

arg :: Expr t -> Function a -> Function a
arg (Expr v) f = f { functionArgs = functionArgs f ++ [v]}

function :: ByteString -> Function a
function bs = Function bs []

call :: Function a -> Expr a
call (Function bs args) = Expr  $
   raw bs <> raw "(" <> commaSep (map args) <> raw ")"

-}

data QueryState = QueryState
  { queryStateWith   :: Maybe ExprBuilder
  , queryStateFrom   :: Maybe ExprBuilder
  , queryStateWhere  :: Maybe ExprBuilder
  , queryStateOrder  :: Maybe ExprBuilder
  , queryStateLimit  :: Maybe Int
  , queryStateOffset :: Maybe Int
  }

instance Monoid QueryState where
  mempty = QueryState Nothing Nothing Nothing Nothing Nothing Nothing
  QueryState wa fa wha sa la oa `mappend` QueryState wb fb whb sb lb ob =
        QueryState (joinWith (raw ",") wa wb)
                   (joinWith (raw ",") fa fb)
                   (joinWith (raw " AND ") wha whb)
                   (joinWith (raw ",") sa sb)
                   (getLast (Last la <> Last lb))
                   (getLast (Last oa <> Last ob))
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

compIt :: MonadState NameSource m => Query t -> m (t, QueryState)
compIt (Query a) = do
  ns <- get
  let ((r, ns'), q) = runWriter (runStateT a ns)
  put ns'
  return (r,q)

finishIt :: ExprBuilder -> QueryState -> ExprBuilder
finishIt expr q =
        opt (raw " WITH " `fprepend` queryStateWith q)
        <> raw "SELECT " <> expr
        <> opt (raw " FROM "`fprepend` queryStateFrom q)
        <> opt (raw " WHERE " `fprepend` queryStateWhere q)
        <> opt (raw " ORDER BY " `fprepend` queryStateOrder q)
        <> opt (mappend (raw " LIMIT ") . rawField <$> queryStateLimit q)
        <> opt (mappend (raw " OFFSET ") .rawField <$> queryStateOffset q)

compileQuery :: Query ExprBuilder -> QueryM ExprBuilder
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

whereQ :: Expr t -> Query ()
whereQ (Expr r) = Query $ do
  lift $ tell mempty {queryStateWhere = Just r}

type FromM a = State NameSource a


data FromD a where
   FromTable      :: ExprBuilder -> a -> FromD a
   FromInnerJoin  :: FromD a -> FromD b -> ExprBuilder -> FromD (a:.b)
   FromCrossJoin  :: FromD a -> FromD b -> FromD (a:.b)


fromExpr :: FromD a -> a
fromExpr (FromTable _ a) = a
fromExpr (FromInnerJoin a b _) = fromExpr a :. fromExpr b
fromExpr (FromCrossJoin a b) = fromExpr a :. fromExpr b

newtype From a = From { runFrom :: State NameSource (FromD a) }


compileFrom :: FromD a -> (ExprBuilder, a)
compileFrom (FromTable bs a) = (bs, a)

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
      compiledSel = finishIt (commaSep exprs) $ insertWriterFrom q
      res = raw "INSERT INTO "<>table <> raw " (" <> compiledSel <> raw ")"
  return (r,res)

newtype Updating r a = Updating {
        runUpdating :: StateT NameSource (Writer InsertWriter) a
    } deriving (Monad, Functor, Applicative)


finishUpdating :: [(ByteString, ExprBuilder)] -> ExprBuilder -> QueryState -> ExprBuilder
finishUpdating setters table q =
        opt (raw " WITH " `fprepend` queryStateWith q)
        <> raw "UPDATE TABLE "<> table
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
  let (cols, exprs) = unzip $ insertWriterSets q
      compiled = finishUpdating (insertWriterSets q) table $ insertWriterFrom q
  return (r,compiled)

newtype Update a = Update { runUpdate :: State NameSource (a, ExprBuilder) }
