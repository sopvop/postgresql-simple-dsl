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
import           Data.Maybe                           (catMaybes)
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

builder :: Builder -> DList Action
builder = D.singleton . Plain

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


quoted :: Builder -> Builder
quoted bld = B.fromChar '"' <> bld <> B.fromChar '"'

mkAccess :: Rel t -> ByteString -> DList Action
mkAccess (Rel r) fld = builder $
      quoted r <> B.fromChar '.' <> quoted (B.fromByteString fld)
mkAccess (RelTable r) fld = builder $
      quoted r <> B.fromChar '.' <> quoted (B.fromByteString fld)
mkAccess (RelAliased r ) fld = builder . quoted
         $ r <> B.fromChar '.' <>  B.fromByteString fld


commaSep :: [ExprBuilder] -> ExprBuilder
commaSep = D.concat . intersperse (D.singleton . Plain $ B.fromChar ',')

data Rel r = Rel Builder
           | RelTable Builder
           | RelAliased Builder

-- | Source of uniques
newtype NameSource = NameSource { getNameSource :: Int }

-- | Select query
mkRename :: ExprBuilder -> ExprBuilder -> ExprBuilder
mkRename s nm = s <> raw " AS " <> nm

class IsRecord a where
  type FromRecord a :: *
  asValues  :: a -> [ExprBuilder]
  asRenamed :: MonadState NameSource m => a -> m a

instance Record a => IsRecord (Rel a) where
  type FromRecord (Rel a) = Whole a
  asRenamed (RelTable nm) = do
    nm' <- grabAlias nm
    return $ RelAliased nm'
  asRenamed (Rel nm) = return $ RelAliased nm
  asRenamed a = return a
  asValues rel = map (mkAccess rel) . recordColumns $ recordParser (Proxy :: Proxy a)

instance IsRecord (Expr (Only a)) where
  type FromRecord (Expr (Only a)) = Only a
  asValues (Expr a) = [a]
  asRenamed _ = liftM Expr grabName

instance IsRecord (Expr a, Expr b) where
  type FromRecord (Expr a, Expr b) = (a, b)
  asValues (Expr a, Expr b) = [a,b]
  asRenamed _ = return (,) `ap` grabExpr `ap` grabExpr

instance IsRecord (Expr a, Expr b, Expr c) where
  type FromRecord (Expr a, Expr b, Expr c) = (a, b, c)
  asValues (Expr a, Expr b, Expr c) = [a,b,c]
  asRenamed _ = return (,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr

instance IsRecord (Expr a, Expr b, Expr c, Expr d) where
  type FromRecord (Expr a, Expr b, Expr c, Expr d) = (a, b, c, d)
  asValues (Expr a, Expr b, Expr c, Expr d) = [a,b,c,d]
  asRenamed _ = return (,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr

instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e) where
  type FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e) = (a, b, c, d, e)
  asValues (Expr a, Expr b, Expr c, Expr d, Expr e) = [a,b,c,d,e]
  asRenamed _ = return (,,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr
                `ap` grabExpr

instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) where
  type FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = (a, b, c, d, e, f)
  asValues (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = [a,b,c,d,e,f]
  asRenamed _ = return (,,,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr
                `ap` grabExpr `ap` grabExpr

instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) where
  type FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
       = (a, b, c, d, e, f, g)
  asValues (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) = [a,b,c,d,e,f,g]
  asRenamed _ = return (,,,,,,) `ap` grabExpr `ap` grabExpr `ap` grabExpr `ap` grabExpr
                `ap` grabExpr `ap` grabExpr `ap` grabExpr

instance (IsRecord a, IsRecord b) => IsRecord (a:.b) where
  type FromRecord (a :. b) = (FromRecord a :. FromRecord b)
  asValues (a:.b) = asValues a ++ asValues b
  asRenamed ~(a:.b) = return (:.) `ap` asRenamed a `ap` asRenamed b

class ToRecord a where
  type AsRecord a :: *
  toRecord :: a -> AsRecord a

instance ToField a => ToRecord (Only a) where
  type AsRecord (Only a) = Expr (Only a)
  toRecord (Only a) = Expr $ rawField a

instance (ToField a, ToField b) => ToRecord (a,b) where
  type AsRecord (a,b) = (Expr a, Expr b)
  toRecord (a,b) = (Expr $ rawField a, Expr $ rawField b)

instance (ToField a, ToField b, ToField c) => ToRecord (a,b,c) where
  type AsRecord (a,b,c) = (Expr a, Expr b, Expr c)
  toRecord (a,b,c) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c)

instance (ToField a, ToField b, ToField c, ToField d) => ToRecord (a,b,c,d) where
  type AsRecord (a,b,c,d) = (Expr a, Expr b, Expr c, Expr d)
  toRecord (a,b,c,d) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                      Expr $ rawField d)

instance (ToField a, ToField b, ToField c, ToField d, ToField e) => ToRecord (a,b,c,d,e) where
  type AsRecord (a,b,c,d,e) = (Expr a, Expr b, Expr c, Expr d, Expr e)
  toRecord (a,b,c,d,e) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                        Expr $ rawField d, Expr $ rawField e)

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f)
         => ToRecord (a,b,c,d,e,f) where
  type AsRecord (a,b,c,d,e,f) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  toRecord (a,b,c,d,e,f) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                          Expr $ rawField d, Expr $ rawField e, Expr $ rawField f)

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f, ToField g)
         => ToRecord (a,b,c,d,e,f,g) where
  type AsRecord (a,b,c,d,e,f,g) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
  toRecord (a,b,c,d,e,f,g) = (Expr $ rawField a, Expr $ rawField b, Expr $ rawField c,
                            Expr $ rawField d, Expr $ rawField e, Expr $ rawField f,
                            Expr $ rawField g)

instance (ToRecord a, ToRecord b) => ToRecord (a:.b) where
  type AsRecord (a:.b) = AsRecord a :. AsRecord b
  toRecord (a:.b) = toRecord a :. toRecord b

class IsRecord (AggRecord a) => IsAggregate a where
  type AggRecord a :: *
  fromAggr :: a -> AggRecord a
  compileGroupBy :: a -> [ExprBuilder]

data ExprA a = ExprGrp ExprBuilder
                | ExprAgg ExprBuilder

aggrBld :: ExprA t -> ExprBuilder
aggrBld (ExprGrp b) = b
aggrBld (ExprAgg b) = b

compAgg :: ExprA t -> Maybe ExprBuilder
compAgg (ExprGrp b) = Just b
compAgg _ = Nothing

instance IsAggregate (ExprA (Only a)) where
  type AggRecord (ExprA (Only a)) = Expr (Only a)
  fromAggr = Expr . aggrBld
  compileGroupBy (ExprGrp b) = [b]
  compileGroupBy _ = []

instance IsAggregate (ExprA a, ExprA b) where
  type AggRecord (ExprA a, ExprA b) = (Expr a, Expr b)
  fromAggr (a, b) = (Expr $ aggrBld a, Expr $ aggrBld b)
  compileGroupBy (a,b) = catMaybes [compAgg a, compAgg b]

instance IsAggregate (ExprA a, ExprA b, ExprA c) where
  type AggRecord (ExprA a, ExprA b, ExprA c) = (Expr a, Expr b, Expr c)
  fromAggr (a, b, c) = (Expr $ aggrBld a, Expr $ aggrBld b, Expr $ aggrBld c)
  compileGroupBy (a,b,c) = catMaybes [compAgg a, compAgg b, compAgg c]
instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d) = (Expr a, Expr b, Expr c, Expr d)
  fromAggr (a, b, c, d) = (Expr $ aggrBld a, Expr $ aggrBld b, Expr $ aggrBld c,
                           Expr $ aggrBld d)
  compileGroupBy (a,b,c,d) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d]

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e) =
       (Expr a, Expr b, Expr c, Expr d, Expr e)
  fromAggr (a, b, c, d, e) = (Expr $ aggrBld a, Expr $ aggrBld b, Expr $ aggrBld c,
                           Expr $ aggrBld d, Expr $ aggrBld e)
  compileGroupBy (a,b,c,d,e) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d
                                         ,compAgg e]

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f) =
       (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  fromAggr (a, b, c, d, e, f) = (Expr $ aggrBld a, Expr $ aggrBld b, Expr $ aggrBld c,
                                 Expr $ aggrBld d, Expr $ aggrBld e, Expr $ aggrBld f)
  compileGroupBy (a,b,c,d,e,f) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d
                                           ,compAgg e, compAgg f]

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f, ExprA g) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f, ExprA g) =
       (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
  fromAggr (a, b, c, d, e, f, g) = (Expr $ aggrBld a, Expr $ aggrBld b, Expr $ aggrBld c,
                                    Expr $ aggrBld d, Expr $ aggrBld e, Expr $ aggrBld f,
                                    Expr $ aggrBld g)
  compileGroupBy (a,b,c,d,e,f,g) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d
                                             ,compAgg e, compAgg f, compAgg g]

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

grabAlias :: MonadState NameSource m => Builder -> m Builder
grabAlias bs = do
  NameSource num <- get
  put (NameSource $ succ num)
  return $ bs <> B.fromChar '_' <> B.fromShow num

grabName :: MonadState NameSource m => m (ExprBuilder)
grabName = liftM (D.singleton . Plain) grabName_

grabExpr :: MonadState NameSource m => m (Expr a)
grabExpr = liftM (Expr . builder . quoted) $ grabAlias (B.fromByteString "field.")

compIt :: MonadState NameSource m => Query t -> m (t, QueryState)
compIt (Query a) = do
  ns <- get
  let ((r, ns'), q) = runWriter (runStateT a ns)
  put ns'
  return (r,q)

finishIt :: [ExprBuilder] -> QueryState -> ExprBuilder
finishIt expr q = finishItAgg expr Nothing q

finishItAgg :: [ExprBuilder] -> Maybe ExprBuilder -> QueryState -> ExprBuilder
finishItAgg expr groupBy q =
        opt (raw withStart `fprepend` queryStateWith q)
        <> raw "\nSELECT " <> commaSep expr
        <> opt (raw "\nFROM "`fprepend` queryStateFrom q)
        <> opt (raw "\nWHERE " `fprepend` queryStateWhere q)
        <> opt (raw "\nORDER BY " `fprepend` queryStateOrder q)
        <> opt (raw "\nGROUP BY " `fprepend` groupBy)
        <> opt (mappend (raw "\nLIMIT ") . rawField <$> queryStateLimit q)
        <> opt (mappend (raw "\nOFFSET ") .rawField <$> queryStateOffset q)
  where
    withStart = if getAny (queryStateRecursive q) then "\nWITH RECURSIVE "
                 else "\nWITH "
compileQuery :: Query [ExprBuilder] -> QueryM ExprBuilder
compileQuery q = do
  (r, q') <- compIt q
  return $ finishIt r q'

finishQuery :: IsRecord a => Query a -> [Action]
finishQuery mq = D.toList res
  where
    res = fst $ runState finisher (NameSource 0)
    finisher = do
      (r, q') <- compIt mq
      return $ finishIt (asValues r) q'

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
   FromTable      :: Builder -> Builder -> a -> FromD a
   FromQuery      :: ExprBuilder -> a -> FromD a
   FromInnerJoin  :: FromD a -> FromD b -> ExprBuilder -> FromD (a:.b)
   FromCrossJoin  :: FromD a -> FromD b -> FromD (a:.b)


fromExpr :: FromD a -> a
fromExpr (FromTable _ _ a) = a
fromExpr (FromQuery _ a) = a
fromExpr (FromInnerJoin a b _) = fromExpr a :. fromExpr b
fromExpr (FromCrossJoin a b) = fromExpr a :. fromExpr b

newtype From a = From { runFrom :: State NameSource (FromD a) }

compileFrom :: FromD a -> (ExprBuilder, a)
compileFrom (FromTable bs alias a) = ( builder $ bs <> B.fromByteString " AS " <> alias, a)
compileFrom (FromQuery bs a) = (bs, a)
compileFrom (FromInnerJoin a b cond) = (bld, ea:.eb)
  where
    (ca, ea) = compileFrom a
    (cb, eb) = compileFrom b
    bld = ca <> raw "\nINNER JOIN " <> cb <> raw " ON " <> cond

compileFrom (FromCrossJoin a b) = (ca <> raw "\nCROSS JOIN " <> cb, ea:.eb)
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


compileInserting :: MonadState NameSource m => ExprBuilder -> Inserting t r
                 -> m (r, ExprBuilder)
compileInserting table (Inserting a) = do
  ns <- get
  let ((r, ns'), q) = runWriter (runStateT a ns)
  put ns'
  let (cols, exprs) = unzip $ insertWriterSets q
      sel = insertWriterFrom q
      withPart = queryStateWith sel
      compiledSel = finishIt exprs $ sel { queryStateWith = Nothing }
      res = opt (raw "\nWITH " `fprepend` withPart)
          <> raw "\nINSERT INTO "<>table <> raw "(" <> commaSep (map raw cols)
          <> raw ") (" <> compiledSel <> raw ")"
  return (r,res)

newtype Updating r a = Updating {
        runUpdating :: StateT NameSource (Writer InsertWriter) a
    } deriving (Monad, Functor, Applicative)


finishUpdating :: [(ByteString, ExprBuilder)] -> ExprBuilder -> QueryState -> ExprBuilder
finishUpdating setters table q =
        opt (raw "\nWITH " `fprepend` queryStateWith q)
        <> raw "\nUPDATE "<> table
        <> raw "\nSET " <> sets
        <> opt (raw "\nFROM "`fprepend` queryStateFrom q)
        <> opt (raw "\nWHERE " `fprepend` queryStateWhere q)
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
  let res = opt (raw "\nWITH " `fprepend` queryStateWith q)
        <> raw "\nDELETE FROM "<> table
        <> opt (raw "\nUSING "`fprepend` queryStateFrom q)
        <> opt (raw "\nWHERE " `fprepend` queryStateWhere q)
  return (expr, res)

newtype Update a = Update { runUpdate :: State NameSource (a, ExprBuilder) }

finishUpdate :: IsRecord t => Update t -> Action
finishUpdate (Update u) = Many . D.toList . fst $ runState (u >>= compiler) (NameSource 0)
  where
    compiler (expr, bld) = do
      return $ bld <> raw "\nRETURNING "<> commaSep (asValues expr)

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

callAgg (Function bs args) = ExprAgg $
   raw bs <> raw "(" <> commaSep args <> raw ")"

data Union = UnionDistinct | UnionAll deriving (Eq, Show, Ord)

data Recursive a = Recursive Union (Query a) (a -> Query a)
