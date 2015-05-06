{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}


module Database.PostgreSQL.Simple.Dsl.Internal
       where

import           Control.Applicative
import           Control.Monad.State.Class
import           Control.Monad.Trans.State            (State, evalState,
                                                       runState)
import           Control.Monad.Trans.Writer
import           Data.ByteString                      (ByteString)
import qualified Data.ByteString.Char8                as B
import           Data.Coerce
import           Control.Monad.Identity
import           Data.HashMap.Strict                  (HashMap)
import qualified Data.HashMap.Strict                  as HashMap
import           Data.List                            (intersperse)
import           Data.Monoid
import           Data.Proxy                           (Proxy (..))
import           Data.Text                            (Text)
import qualified Data.Text.Encoding                   as T

import           Data.Vinyl.Core (Rec(..), (<+>), rappend)

import           Data.ByteString.Builder (Builder, char8, byteString)

import           Database.PostgreSQL.Simple           ((:.) (..), Only (..))
import           Database.PostgreSQL.Simple.FromField as PG hiding (Field)
import           Database.PostgreSQL.Simple.FromRow   as PG
import           Database.PostgreSQL.Simple.ToField

import           Database.PostgreSQL.Simple.Dsl.Types
import           Database.PostgreSQL.Simple.Dsl.Escaping


type ExprBuilder = Builder

-- | Adds phantom type to RawExpr
data Expr a = Expr {-# UNPACK #-} !Int ExprBuilder

getRawExpr :: Expr t -> ExprBuilder
getRawExpr (Expr _ b) = b

term :: ExprBuilder -> Expr a
term = Expr 0

parenPrec :: Bool -> ExprBuilder -> ExprBuilder
parenPrec True bld = addParens bld
parenPrec  _    bld = bld

instance ToField (Expr a) where
  toField (Expr _ a) = Plain a


addParens :: ExprBuilder -> ExprBuilder
addParens t = char8 '(' <> t <> char8 ')'

opt :: Monoid a => Maybe a -> a
opt Nothing = mempty
opt (Just x) = x

fprepend :: (Functor f, Monoid b) => b -> f b -> f b
fprepend p a = (p <>) <$> a
fappend :: (Functor f, Monoid b) => f b -> b -> f b
fappend a x = (<> x) <$> a

rawField :: ToField a => a -> ExprBuilder
rawField = escapeAction . toField


binOp :: IsExpr expr => Int -> ExprBuilder -> expr a -> expr b -> expr c
binOp p op ea eb = fromExpr . Expr p $
   parenPrec (p < pa) a <> op <> parenPrec (p < pb) b
  where
    (Expr pa a) = toExpr ea
    (Expr pb b) = toExpr eb
{-# INLINE binOp #-}

prefOp :: IsExpr expr => Int -> ExprBuilder -> expr a -> expr b
prefOp p op ea = fromExpr . Expr p $ op <> parenPrec (p < pa) a
  where
    (Expr pa a) = toExpr ea
{-# INLINE prefOp #-}

commaSep :: [ExprBuilder] -> ExprBuilder
commaSep = mconcat . intersperse (char8 ',')

-- | Source of uniques

newtype NameSource = NameSource Int

mkNameSource :: NameSource
mkNameSource = NameSource 0
grabNameFromSrc :: NameSource -> (String, NameSource)
grabNameFromSrc (NameSource n) = ('_':'_':'q': show n, NameSource $ succ n)

instance Show NameSource where
  show _ = "NameSource"

class (Monad m) => HasNameSource m where
  grabNS :: m NameSource
  modifyNS :: (NameSource -> NameSource) -> m ()

class IsRecord a where
  asValues  :: a -> [ExprBuilder]
  asRenamed :: HasNameSource m => a -> m a

-- | Parser for entities with columns
data RecordParser a = RecordParser
  { recordColumns   :: [ExprBuilder]
  , recordRowParser :: RowParser a
  }

instance Functor RecordParser where
  fmap f (RecordParser cs p) = RecordParser cs (fmap f p)

instance Applicative RecordParser where
  pure =  RecordParser [] . pure
  RecordParser cf f <*> RecordParser ca a = RecordParser (cf <> ca) (f <*> a)

recField :: FromField a => Expr a -> RecordParser a
recField (Expr _ e) = RecordParser [e] field

takeField :: FromField a => Expr a -> RecordParser a
takeField = recField

class FromRecord a b where
  fromRecord :: a -> RecordParser b

instance IsRecord (Expr a) where
  asValues (Expr _ a) = [a]
  asRenamed _ = liftM term newName_

instance (FromField a) => FromRecord (Expr a) a where
  fromRecord (Expr p e) = recField $ Expr p e

instance IsRecord (Expr a, Expr b) where
  asValues (Expr _ a, Expr _ b) = [a,b]
  asRenamed _ = return (,) `ap` newExpr `ap` newExpr

instance (FromField a, FromField b) => FromRecord (Expr a, Expr b) (a,b) where
  fromRecord (a,b) = (,) <$> recField a <*> recField b

instance IsRecord (Expr a, Expr b, Expr c) where
  asValues (Expr _ a, Expr _ b, Expr _ c) = [a,b,c]
  asRenamed _ = return (,,) `ap` newExpr `ap` newExpr `ap` newExpr

instance (FromField a, FromField b, FromField c) =>
         FromRecord (Expr a, Expr b, Expr c) (a,b,c) where
  fromRecord (a,b,c) = (,,) <$> recField a <*> recField b <*> recField c

instance IsRecord (Expr a, Expr b, Expr c, Expr d) where
  asValues (Expr _ a, Expr _ b, Expr _ c, Expr _ d) = [a,b,c,d]
  asRenamed _ = return (,,,) `ap` newExpr `ap` newExpr `ap` newExpr `ap` newExpr

instance (FromField a, FromField b, FromField c, FromField d) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d) (a,b,c,d) where
  fromRecord (a,b,c,d) = (,,,) <$> recField a <*> recField b <*> recField c <*> recField d

instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e) where
  asValues (Expr _ a, Expr _ b, Expr _ c, Expr _ d, Expr _ e) = [a,b,c,d,e]
  asRenamed _ = return (,,,,) `ap` newExpr `ap` newExpr `ap` newExpr `ap` newExpr
                `ap` newExpr

instance (FromField a, FromField b, FromField c, FromField d, FromField e) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e) (a,b,c,d, e) where
  fromRecord (a,b,c,d,e) = (,,,,) <$> recField a <*> recField b <*> recField c
                                  <*> recField d <*> recField e

instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) where
  asValues (Expr _ a, Expr _ b, Expr _ c, Expr _ d, Expr _ e, Expr _ f) = [a,b,c,d,e,f]
  asRenamed _ = return (,,,,,) `ap` newExpr `ap` newExpr `ap` newExpr `ap` newExpr
                `ap` newExpr `ap` newExpr

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) (a,b,c,d,e, f) where
  fromRecord (a,b,c,d,e,f) = (,,,,,) <$> recField a <*> recField b <*> recField c
                                     <*> recField d <*> recField e <*> recField f


instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g) where
  asValues (Expr _ a, Expr _ b, Expr _ c, Expr _ d, Expr _ e, Expr _ f, Expr _ g) = [a,b,c,d,e,f,g]
  asRenamed _ = return (,,,,,,) `ap` newExpr `ap` newExpr `ap` newExpr `ap` newExpr
                `ap` newExpr `ap` newExpr `ap` newExpr

instance (FromField a, FromField b, FromField c, FromField d
         , FromField e, FromField f, FromField g) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
         (a,b,c,d,e,f,g) where
  fromRecord (a,b,c,d,e,f,g) = (,,,,,,) <$> recField a <*> recField b <*> recField c
                                       <*> recField d <*> recField e <*> recField f
                                       <*> recField g

instance (IsRecord a, IsRecord b) => IsRecord (a:.b) where
  asValues (a:.b) = asValues a ++ asValues b
  asRenamed ~(a:.b) = return (:.) `ap` asRenamed a `ap` asRenamed b

instance (FromRecord a a', FromRecord b b') => FromRecord (a :. b) (a':.b') where
  fromRecord (a:.b) = (:.) <$> fromRecord a <*> fromRecord b

class ToRecord a where
  type AsRecord a :: *
  toRecord :: a -> AsRecord a

instance ToField a => ToRecord (Only a) where
  type AsRecord (Only a) = Expr a
  toRecord (Only a) = term $ rawField a

instance (ToField a, ToField b) => ToRecord (a,b) where
  type AsRecord (a,b) = (Expr a, Expr b)
  toRecord (a,b) = (term $ rawField a, term $ rawField b)

instance (ToField a, ToField b, ToField c) => ToRecord (a,b,c) where
  type AsRecord (a,b,c) = (Expr a, Expr b, Expr c)
  toRecord (a,b,c) = (term $ rawField a, term $ rawField b, term $ rawField c)

instance (ToField a, ToField b, ToField c, ToField d) => ToRecord (a,b,c,d) where
  type AsRecord (a,b,c,d) = (Expr a, Expr b, Expr c, Expr d)
  toRecord (a,b,c,d) = (term $ rawField a, term $ rawField b, term $ rawField c,
                      term $ rawField d)

instance (ToField a, ToField b, ToField c, ToField d, ToField e) => ToRecord (a,b,c,d,e) where
  type AsRecord (a,b,c,d,e) = (Expr a, Expr b, Expr c, Expr d, Expr e)
  toRecord (a,b,c,d,e) = (term $ rawField a, term $ rawField b, term $ rawField c,
                        term $ rawField d, term $ rawField e)

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f)
         => ToRecord (a,b,c,d,e,f) where
  type AsRecord (a,b,c,d,e,f) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  toRecord (a,b,c,d,e,f) = (term $ rawField a, term $ rawField b, term $ rawField c,
                          term $ rawField d, term $ rawField e, term $ rawField f)

instance (ToField a, ToField b, ToField c, ToField d, ToField e, ToField f, ToField g)
         => ToRecord (a,b,c,d,e,f,g) where
  type AsRecord (a,b,c,d,e,f,g) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
  toRecord (a,b,c,d,e,f,g) = (term $ rawField a, term $ rawField b, term $ rawField c,
                            term $ rawField d, term $ rawField e, term $ rawField f,
                            term $ rawField g)

instance (ToRecord a, ToRecord b) => ToRecord (a:.b) where
  type AsRecord (a:.b) = AsRecord a :. AsRecord b
  toRecord (a:.b) = toRecord a :. toRecord b


class IsExpr expr where
  toExpr :: expr a -> Expr a
  fromExpr :: Expr a -> expr a

instance IsExpr Expr where
  toExpr = id
  fromExpr = id
  {-# INLINE toExpr #-}
  {-# INLINE fromExpr #-}

instance IsExpr ExprA where
  toExpr (ExprA a) = a
  fromExpr = ExprA
  {-# INLINE toExpr #-}
  {-# INLINE fromExpr #-}

newtype ExprA a = ExprA { fromExprA :: Expr a }

class IsRecord (AggRecord a) => IsAggregate a where
  type AggRecord a :: *
  fromAggr :: a -> AggRecord a

instance IsAggregate (ExprA a) where
  type AggRecord (ExprA a) = Expr a
  fromAggr = coerce

instance IsAggregate (ExprA a, ExprA b) where
  type AggRecord (ExprA a, ExprA b) = (Expr a, Expr b)
  fromAggr = coerce

instance IsAggregate (ExprA a, ExprA b, ExprA c) where
  type AggRecord (ExprA a, ExprA b, ExprA c) = (Expr a, Expr b, Expr c)
  fromAggr = coerce

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d) = (Expr a, Expr b, Expr c, Expr d)
  fromAggr = coerce

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e) =
       (Expr a, Expr b, Expr c, Expr d, Expr e)
  fromAggr = coerce

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f) =
       (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  fromAggr = coerce

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f, ExprA g) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f, ExprA g) =
       (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
  fromAggr = coerce



data Distinct = DistinctAll | Distinct | DistinctOn ExprBuilder

data QueryState t = QueryState
  { queryStateWith      :: Maybe ExprBuilder
  , queryStateDistinct  :: Distinct
  , queryStateRecursive :: Any
  , queryStateFrom      :: Maybe ExprBuilder
  , queryStateWhere     :: Maybe ExprBuilder
  , queryStateOrder     :: Maybe ExprBuilder
  , queryStateGroupBy   :: Maybe ExprBuilder
  , queryStateHaving    :: Maybe ExprBuilder
  , queryStateLimit     :: Maybe Int
  , queryStateOffset    :: Maybe Int
  , queryAction         :: t
  , queryNameSource     :: !NameSource
  }

emptyQuery :: Monoid t => QueryState t
emptyQuery = emptyQueryWith mkNameSource

emptyQueryWith :: Monoid t => NameSource -> QueryState t
emptyQueryWith = QueryState mempty DistinctAll mempty mempty mempty mempty mempty mempty
                 Nothing Nothing mempty

instance HasNameSource (QueryM t) where
  grabNS = getNameSource
  modifyNS = modifyNameSource
  {-# INLINE grabNS #-}
  {-# INLINE modifyNS #-}

grabName :: HasNameSource m => m ByteString
grabName = do
  NameSource n <- grabNS
  modifyNS . const . NameSource $ succ n
  return $ B.pack  $ '_':'_':'q': show n

{-# INLINE grabName #-}

newExpr :: HasNameSource m => m (Expr a)
newExpr = liftM term  $ newName_
{-# INLINE newExpr #-}

newName_ :: HasNameSource m => m Builder
newName_ = do
  nm <- grabName
  return $ byteString nm
{-# INLINE newName_ #-}


newtype QueryM t a = QueryM { runQuery :: State (QueryState t) a }
        deriving (Functor, Monad, Applicative, MonadState (QueryState t))

type Query a = QueryM () a

class (HasNameSource m, Monad m) => IsQuery m where
  modifyWith :: (Maybe ExprBuilder -> Maybe ExprBuilder) -> m ()
  modifyFrom :: (Maybe ExprBuilder -> Maybe ExprBuilder) -> m ()
  modifyWhere :: (Maybe ExprBuilder -> Maybe ExprBuilder) -> m ()
  modifyNameSource :: (NameSource -> NameSource) -> m ()
  getNameSource :: m NameSource

instance IsQuery (QueryM t) where
  modifyWith f = do
    st <- get
    put $ st { queryStateWith = f (queryStateWith st ) }

  modifyFrom f = do
    st <- get
    put $ st { queryStateFrom = f (queryStateFrom st ) }

  modifyWhere f = do
    st <- get
    put $ st { queryStateWhere = f (queryStateWhere st ) }

  modifyNameSource f = do
    st <- get
    put $ st { queryNameSource = f (queryNameSource st) }

  getNameSource = do
    st <- get
    return $ queryNameSource st

  {-# INLINE modifyWith #-}
  {-# INLINE modifyFrom #-}
  {-# INLINE modifyWhere #-}
  {-# INLINE modifyNameSource #-}
  {-# INLINE getNameSource #-}

modifyLimit :: MonadState (QueryState t) m => (Maybe Int -> Maybe Int) -> m ()
modifyLimit f = do
   st <- get
   put $ st { queryStateLimit = f (queryStateLimit st) }

modifyOffset :: MonadState (QueryState t) m => (Maybe Int -> Maybe Int) -> m ()
modifyOffset f = do
   st <- get
   put $ st { queryStateOffset = f (queryStateOffset st) }

modifyDistinct  :: MonadState (QueryState t) m => (Distinct -> Distinct) -> m ()
modifyDistinct f = do
   st <- get
   put $ st { queryStateDistinct = f (queryStateDistinct st) }
{-# INLINE modifyDistinct #-}
{-# INLINE modifyLimit #-}
{-# INLINE modifyOffset #-}

modifyRecursive :: MonadState (QueryState t) m => (Bool -> Bool) -> m ()
modifyRecursive f = do
  st <- get
  put $ st { queryStateRecursive = Any $ f (getAny $ queryStateRecursive st ) }
{-# INLINE modifyRecursive #-}

appendWith :: IsQuery m => ExprBuilder -> m ()
appendWith act = modifyWith $ \w ->
  (w `fappend` ",\n") <> Just act

{-# INLINE appendWith #-}

appendFrom :: IsQuery m => ExprBuilder -> m ()
appendFrom f = modifyFrom $ \frm ->
  (frm `fappend` ",\n") <> Just f
{-# INLINE appendFrom #-}

appendWhere :: IsQuery m => ExprBuilder -> m ()
appendWhere w = modifyWhere $ \wh ->
  (wh `fappend` " AND ") <> Just w
{-# INLINE appendWhere #-}

appendWhereExpr :: IsQuery m => Expr t -> m ()
appendWhereExpr (Expr pre w) = modifyWhere $ \wh ->
    Just $ maybe wprep (\wh' -> (wh' <> " AND ") <> wprep) wh
  where
    wprep = parenPrec (18 < pre) w

{-# INLINE appendWhereExpr #-}

--compIt :: (Monoid t) => QueryM t b -> QueryM s (b, QueryState t)
compIt :: (HasNameSource m, Monoid t) => QueryM t b -> m (b, QueryState t)
compIt (QueryM a) = do
  ns <- grabNS
  let (res, st') = runState a $ emptyQueryWith $ ns
  modifyNS $ const (queryNameSource st')
  return (res, st')
{-# INLINE compIt #-}

newtype Aggregator a = Aggregator { runAggregator :: Query a }
        deriving (Functor, Applicative, Monad, HasNameSource)

appendHaving :: ExprBuilder -> Aggregator ()
appendHaving e = Aggregator . modify $ \st ->
      st { queryStateHaving = (queryStateHaving st `fappend` " AND ") <> Just e }
{-# INLINE appendHaving #-}

appendGroupBy :: ExprBuilder -> Aggregator ()
appendGroupBy e = Aggregator . modify $ \st ->
   st { queryStateGroupBy = (queryStateGroupBy st `fappend` char8 ',') <> Just e }

{-# INLINE appendGroupBy #-}

finishIt :: [ExprBuilder] -> QueryState t -> ExprBuilder
finishIt expr QueryState{..} = execWriter $ do
  forM_ queryStateWith $ \w -> tell (withStart <> w)
  tell $ ("\nSELECT ")
  tell $ distinct queryStateDistinct
  tell $ commaSep expr
  forM_ queryStateFrom $ \f -> tell ("\nFROM " <> f)
  forM_ queryStateWhere $ \w -> tell ("\nWHERE " <> w)
  forM_ queryStateOrder $ \o -> tell ("\nORDER BY " <> o)
  forM_ queryStateGroupBy $ \b -> do
     tell $ "\nGROUP BY " <> b
  forM_ queryStateHaving $ \b -> do
     tell $ "\nHAVING " <> b
  forM_ queryStateLimit $ \l -> do
     tell $ "\nLIMIT " <> escapeAction (toField l)
  forM_ queryStateOffset $ \o -> do
     tell $ "\nOFFSET " <> escapeAction (toField o)
  where
    distinct d = case d of
      DistinctAll -> mempty
      Distinct -> "DISTINCT "
      DistinctOn e -> "DISTINCT ON (" <> e <>  ") "
    withStart = if getAny queryStateRecursive then "\nWITH RECURSIVE "
                 else "\nWITH "

compileQuery :: forall a t . (IsRecord a, Monoid t) => QueryM t a -> ExprBuilder
compileQuery q = evalState comp (emptyQuery :: QueryState t)
 where
   comp = runQuery $ do
     (r,q') <- compIt q
     return $ finishIt (asValues r) q'

data FromD a where
   FromTable      :: ByteString -> Builder -> a -> FromD a
   FromQuery      :: ExprBuilder -> a -> FromD a
   FromInnerJoin  :: FromD a -> FromD b -> ExprBuilder -> FromD (a:.b)
   FromCrossJoin  :: FromD a -> FromD b -> FromD (a:.b)

data Table a = Table !Text

-- | class for things which can be used in FROM clause
class FromItem f a | f -> a where
   fromItem :: f -> From a

instance FromItem (From a) a where
   fromItem = id

instance (IsRecord a) => FromItem (Query a) a where
  fromItem mq = From $ do
     (r, q) <- compIt mq
     nm <- newName_
     renamed <- asRenamed r
     let bld = char8 '(' <> finishIt (asValues r) q <> ") AS "
               <> namedRow nm renamed
     return $ FromQuery bld renamed


fromToExpr :: FromD a -> a
fromToExpr (FromTable _ _ a) = a
fromToExpr (FromQuery _ a) = a
fromToExpr (FromInnerJoin a b _) = fromToExpr a :. fromToExpr b
fromToExpr (FromCrossJoin a b) = fromToExpr a :. fromToExpr b
{-# INLINE fromToExpr #-}

newtype From a = From { runFrom :: State NameSource (FromD a) }

namedRow :: IsRecord a => ExprBuilder -> a -> ExprBuilder
namedRow nm r = nm <> char8 '(' <> commaSep (asValues r) <> char8 ')'

instance HasNameSource (State NameSource) where
  grabNS = get
  modifyNS f = modify f
  {-# INLINE grabNS #-}
  {-# INLINE modifyNS #-}

compileFrom :: FromD a -> (ExprBuilder, a)
compileFrom (FromTable bs alias a) =
  (escapeIdentifier bs <> " AS " <> alias, a)
compileFrom (FromQuery bs a) = (bs, a)
compileFrom (FromInnerJoin a b cond) = (bld, ea:.eb)
  where
    (ca, ea) = compileFrom a
    (cb, eb) = compileFrom b
    bld = ca <> "\nINNER JOIN " <> cb <> " ON " <> cond

compileFrom (FromCrossJoin a b) = (ca <> ", " <> cb, ea:.eb)
  where
    (ca, ea) = compileFrom a
    (cb, eb) = compileFrom b


finishQuery :: (a -> RecordParser b) -> Query a -> (ExprBuilder, RowParser b)
finishQuery fromRec mq = evalState finisher $ (emptyQuery :: QueryState ())
  where
    finisher = runQuery $ do (r,q') <- compIt mq
                             let RecordParser cols parser = fromRec r
                             return (finishIt cols q', parser)

finishQueryNoRet :: Query t -> ExprBuilder
finishQueryNoRet mq = evalState finisher (emptyQuery :: QueryState ())
  where
    finisher = runQuery $ do
      (_, q') <- compIt mq
      return $ finishIt ["true"] q'

newtype Sorting = Sorting [ExprBuilder]
  deriving (Monoid)

sortingEmpty :: Sorting -> Bool
sortingEmpty (Sorting []) = True
sortingEmpty _ = False
{-# INLINE sortingEmpty #-}

compileSorting :: Sorting -> ExprBuilder
compileSorting (Sorting r) = mconcat $ intersperse (char8 ',') r
{-# INLINE compileSorting #-}

newtype UpdExpr a = UpdExpr { getUpdates :: HashMap Text ExprBuilder }

instance Monoid (UpdExpr a) where
  mempty = UpdExpr mempty
  UpdExpr a `mappend` UpdExpr b = UpdExpr (a<>b)

newtype Update a = Update { runUpdate :: QueryM (ExprBuilder) a }
   deriving (Functor, Applicative, Monad, HasNameSource)


newtype Inserting r a = Inserting { runInserting :: QueryM [(Text, ExprBuilder)] a }
   deriving (Functor, Applicative, Monad, HasNameSource)


compileInserting :: ExprBuilder -> QueryState [(Text, ExprBuilder)] -> Update ()
compileInserting table q = Update $ do
  let (cols, exprs) = unzip $ queryAction q
      withPart = queryStateWith q
      compiledSel = finishIt exprs $ q { queryStateWith = mempty }
      res = opt ("\nWITH " `fprepend` withPart)
            <> "\nINSERT INTO " <> table <> char8 '('
            <> (commaSep (map escapeIdent cols) <> ") (")
            <> (compiledSel <> ")")
  modify $ \st -> st { queryAction = res }

finishUpdate :: (a -> RecordParser b) -> Update a -> (ExprBuilder, RowParser b)
finishUpdate recParser (Update (QueryM u)) = (result, parser)
  where
    (expr, bld) = runState u emptyQuery
    RecordParser cols parser = recParser expr
    result = queryAction bld <> "\nRETURNING " <> commaSep cols

compileUpdate :: (IsRecord t) => Update t -> ExprBuilder
compileUpdate (Update (QueryM u)) =
    queryAction bld <> "\nRETURNING " <> commaSep (asValues expr)
  where
    (expr, bld) = runState u emptyQuery

finishUpdateNoRet :: Update t -> ExprBuilder
finishUpdateNoRet (Update (QueryM u)) = finish $ runState u emptyQuery
  where
    finish (_, bld) = queryAction bld


newtype Updating r a = Updating { runUpdating :: QueryM (HashMap Text ExprBuilder) a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)


finishUpdating :: ExprBuilder -> QueryState (HashMap Text ExprBuilder) -> ExprBuilder
finishUpdating table QueryState{..} =
         opt ("\nWITH " `fprepend` queryStateWith)
         <> "\nUPDATE " <> table <> "\nSET "
         <> sets
         <> opt ("\nFROM " `fprepend` queryStateFrom)
         <> opt ("\nWHERE " `fprepend` queryStateWhere)
  where
    (columns, exprs) = unzip $ HashMap.toList queryAction
    sets = char8 '(' <> commaSep (map escapeIdent columns)
         <> ")=(" <> commaSep exprs <> char8 ')'


compileDeleting :: ExprBuilder -> Deleting t b -> Update b
compileDeleting table (Deleting del) = Update $ do
   (r, q) <- compIt del
   let bld = opt ("\nWITH " `fprepend` queryStateWith q)
           <> ("\nDELETE FROM " <> table)
           <> opt ("\nUSING " `fprepend` queryStateFrom q)
           <> opt ("\nWHERE " `fprepend` queryStateWhere q)
   modify $ \st -> st { queryAction = bld }
   return r


--compileUpdating :: (MonadState NameSource m) => Action -> Updating t r -> m (r, ExprBuilder)
compileUpdating :: ExprBuilder -> Updating t a -> QueryM ExprBuilder a
compileUpdating table (Updating a) = do
  (r, q) <- compIt a
  let res = finishUpdating table q
  modify $ \st -> st { queryAction = res }
  return r

newtype Deleting t a = Deleting { runDeleting :: QueryM () a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)

data Function a = Function { functionName :: Text
                           , functionArgs :: [ExprBuilder]
                           }

arg :: Expr t -> Function a -> Function a
arg (Expr _ v) f = f { functionArgs = functionArgs f ++ [v]}

function :: Text -> Function a
function bs = Function bs []

escapeIdent :: Text -> Builder
escapeIdent = escapeIdentifier . T.encodeUtf8

call :: Function a -> Expr a
call (Function bs args) = Expr 10 $
     escapeIdent bs <> char8 '(' <> commaSep args <> char8 ')'

callAgg :: Function t -> ExprA a
callAgg (Function bs args) = ExprA . Expr 0 $
   escapeIdent bs <> char8 '(' <> commaSep args <> char8 ')'

data Union = UnionDistinct | UnionAll deriving (Eq, Show, Ord)

data Recursive a = Recursive Union (Query a) (From a -> Query a)

instance IsRecord (PGRecord ((t :-> Expr a) ': '[])) where
  asRenamed (a :& _) = do
    n <- newExpr
    return $ fmap (fmap $ const n) a :& RNil
  asValues (Identity a :& _) = [getRawExpr $ getCol a]

instance (IsRecord (PGRecord (b ': c))) =>
         IsRecord (PGRecord (t :-> Expr a ': b ': c)) where
  asRenamed ( _ :& xs) = do
    n <- newExpr
    res <- asRenamed $ xs
    return $ Proxy =: n <+> res
  asValues (Identity a :& as) = getRawExpr (getCol a) : asValues as

instance FromField a => FromRecord (PGRecord '[t :-> Expr a]) (PGRecord '[ t :-> a]) where
  fromRecord (Identity r :& _) = (Proxy =:) <$> recField (getCol r)

instance (FromRecord (PGRecord (b ': c)) (PGRecord (b' ': c')), FromField a) =>
   FromRecord (PGRecord (t :-> Expr a ': (b ': c)))
   (PGRecord ( t :-> a ': (b' ': c'))) where
  fromRecord (Identity a :& as) = (\a' b' -> Proxy =: a' <+> b') <$> recField (getCol a)
                                                                 <*> fromRecord as

class RecExpr a where
  mkRecExpr :: ExprBuilder -> a -> a

instance NamesColumn t => RecExpr (PGRecord '[t :-> Expr a ]) where
  mkRecExpr b _ = Proxy =: (Expr 0 nm)
    where nm = b <> escapeAction (toField $ columnName (Proxy :: Proxy t))

instance (NamesColumn t, RecExpr (PGRecord (x ': xs)))
         => RecExpr (PGRecord (t :-> Expr a ': x ': xs )) where
  mkRecExpr b _ = Proxy =: (Expr 0 nm)
                `rappend` mkRecExpr b (undefined :: PGRecord ( x ': xs))
    where nm = b <> escapeAction (toField $ columnName (Proxy :: Proxy t))

instance forall a r. (RecExpr (PGRecord a), r~(PGRecord a)) =>
         FromItem (Table (PGRecord a)) (PGRecord a) where
  fromItem (Table nm) = From $ do
    let bs = T.encodeUtf8 nm
        nameBld = escapeIdentifier bs
    alias <- byteString <$> grabName --Alias bs nameBld
    let r = mkRecExpr (alias <> char8 '.') (undefined :: PGRecord a)
        q = nameBld <> " AS " <> alias
    return $ FromQuery q r


update :: forall a b . RecExpr a => Table a -> (a -> Updating a b) -> Update b
update (Table nm) f = Update $ do
  alias <- byteString <$> grabName
  let r = mkRecExpr (alias <> char8 '.') (undefined :: a)
      q = nameBld <> " AS " <> alias
      upd = f r
  compileUpdating q upd
  where
      nameBld = escapeIdentifier $ T.encodeUtf8 nm

delete :: forall a  b . RecExpr a => Table a -> (a -> Deleting a b) -> Update b
delete (Table nm) f = do
  alias <- byteString <$> grabName
  let r = mkRecExpr (alias <> char8 '.') (undefined :: a)
      q = nameBld <> " AS " <> alias
      upd = f r
  compileDeleting q upd
  where
     nameBld = escapeIdentifier $ T.encodeUtf8 nm


insert :: forall a . RecExpr a => Table a -> Query (UpdExpr a) -> Update a
insert (Table nm) mq = do
  let r = mkRecExpr mempty (undefined :: a)
  (UpdExpr upd, q) <- compIt mq
  compileInserting (escapeIdent nm) $ q {queryAction = HashMap.toList upd}
  return r


