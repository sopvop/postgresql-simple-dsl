{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE TypeSynonymInstances       #-}
{-# LANGUAGE UndecidableInstances       #-}

module Database.PostgreSQL.Simple.Dsl.Internal
       where

import           Control.Applicative
import           Control.Monad                        (ap, liftM)
import           Control.Monad.State.Class
import           Control.Monad.Trans.State            (State, evalState,
                                                       runState)
import           Control.Monad.Trans.Writer
import           Data.ByteString                      (ByteString)
import qualified Data.ByteString.Char8                as B
import           Data.DList                           (DList)
import qualified Data.DList                           as D
import           Data.Foldable
import           Data.List                            (intersperse)
import           Data.Maybe                           (catMaybes)
import           Data.Monoid
import           Data.Proxy                           (Proxy (..))
import           Data.Text                            (Text)
import qualified Data.Text                            as T
import qualified Data.Text.Encoding                   as T



import           Blaze.ByteString.Builder             (Builder)
import           Blaze.ByteString.Builder.ByteString  as B
import           Blaze.ByteString.Builder.Char8       as B

import           GHC.TypeLits                         (KnownSymbol, Symbol,
                                                       symbolVal)

import           Database.PostgreSQL.Simple           ((:.) (..), Only (..))
import           Database.PostgreSQL.Simple.FromField as PG hiding (Field)
import           Database.PostgreSQL.Simple.FromRow   as PG
import           Database.PostgreSQL.Simple.ToField


-- | Class for entities which have columns
data family Field v (t :: Symbol) b :: *

fieldSym :: KnownSymbol t => Field v t a -> Proxy (t::Symbol)
fieldSym _ = Proxy

fieldColumn :: KnownSymbol t => Field v t a -> Text
fieldColumn f = T.pack . symbolVal $ fieldSym f

type ExprBuilder = DList Action

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
  toField (Expr _ a) = Many $ D.toList a


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

binOp :: Int -> Action -> Expr a -> Expr b -> Expr c
binOp p op (Expr pa a) (Expr pb b) = Expr p $
   parenPrec (p < pa) a <> D.singleton op <> parenPrec (p < pb) b

prefOp :: Int -> ExprBuilder -> Expr a -> Expr b
prefOp p op (Expr pa a) = Expr p $ op <> parenPrec (p < pa) a

rawC :: Char -> DList Action
rawC c = D.singleton . Plain $ B.fromChar c

mkAccess :: Rel t -> ExprBuilder -> ExprBuilder
mkAccess (RelTable r) fld =  r <> rawC '.' <> fld
mkAccess (RelAliased r ) fld = r <> rawC '_' <> fld


commaSep :: [ExprBuilder] -> ExprBuilder
commaSep = D.concat . intersperse (D.singleton . Plain $ B.fromChar ',')

data Rel r = RelTable ExprBuilder
           | RelAliased ExprBuilder

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

instance (FromRecord (Rel a) a) => IsRecord (Rel a) where
  asRenamed (RelTable _) = do
    nm' <- newName_
    return . RelAliased $ builder nm'
--  asRenamed (RelAliased nm) = return $ RelAliased nm
  asRenamed a = return a
  asValues rel = recordColumns $ (fromRecord rel :: RecordParser a)

instance IsRecord (Expr a) where
  asValues (Expr _ a) = [a]
  asRenamed _ = liftM term newName

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
  type AsRecord (Only a) = Expr (Only a)
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

instance IsAggregate (ExprA a) where
  type AggRecord (ExprA a) = Expr a
  fromAggr = term . aggrBld
  compileGroupBy (ExprGrp b) = [b]
  compileGroupBy _ = []

instance IsAggregate (ExprA a, ExprA b) where
  type AggRecord (ExprA a, ExprA b) = (Expr a, Expr b)
  fromAggr (a, b) = (term $ aggrBld a, term $ aggrBld b)
  compileGroupBy (a,b) = catMaybes [compAgg a, compAgg b]

instance IsAggregate (ExprA a, ExprA b, ExprA c) where
  type AggRecord (ExprA a, ExprA b, ExprA c) = (Expr a, Expr b, Expr c)
  fromAggr (a, b, c) = (term $ aggrBld a, term $ aggrBld b, term $ aggrBld c)
  compileGroupBy (a,b,c) = catMaybes [compAgg a, compAgg b, compAgg c]
instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d) = (Expr a, Expr b, Expr c, Expr d)
  fromAggr (a, b, c, d) = (term $ aggrBld a, term $ aggrBld b, term $ aggrBld c,
                           term $ aggrBld d)
  compileGroupBy (a,b,c,d) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d]

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e) =
       (Expr a, Expr b, Expr c, Expr d, Expr e)
  fromAggr (a, b, c, d, e) = (term $ aggrBld a, term $ aggrBld b, term $ aggrBld c,
                           term $ aggrBld d, term $ aggrBld e)
  compileGroupBy (a,b,c,d,e) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d
                                         ,compAgg e]

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f) =
       (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  fromAggr (a, b, c, d, e, f) = (term $ aggrBld a, term $ aggrBld b, term $ aggrBld c,
                                 term $ aggrBld d, term $ aggrBld e, term $ aggrBld f)
  compileGroupBy (a,b,c,d,e,f) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d
                                           ,compAgg e, compAgg f]

instance IsAggregate (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f, ExprA g) where
  type AggRecord (ExprA a, ExprA b, ExprA c, ExprA d, ExprA e, ExprA f, ExprA g) =
       (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
  fromAggr (a, b, c, d, e, f, g) = (term $ aggrBld a, term $ aggrBld b, term $ aggrBld c,
                                    term $ aggrBld d, term $ aggrBld e, term $ aggrBld f,
                                    term $ aggrBld g)
  compileGroupBy (a,b,c,d,e,f,g) = catMaybes [compAgg a, compAgg b, compAgg c, compAgg d
                                             ,compAgg e, compAgg f, compAgg g]


data Distinct = DistinctAll | Distinct | DistinctOn ExprBuilder

data QueryState t = QueryState
  { queryStateWith      :: Maybe ExprBuilder
  , queryStateDistinct  :: Distinct
  , queryStateRecursive :: Any
  , queryStateFrom      :: Maybe ExprBuilder
  , queryStateWhere     :: Maybe ExprBuilder
  , queryStateOrder     :: Maybe ExprBuilder
  , queryStateLimit     :: Maybe Int
  , queryStateOffset    :: Maybe Int
  , queryAction         :: t
  , queryNameSource     :: !NameSource
  }

emptyQuery :: Monoid t => QueryState t
emptyQuery = emptyQueryWith mkNameSource

emptyQueryWith :: Monoid t => NameSource -> QueryState t
emptyQueryWith = QueryState mempty DistinctAll mempty mempty mempty mempty Nothing Nothing mempty

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
newExpr = liftM (term . builder)  $ newName_
{-# INLINE newExpr #-}

newName_ :: HasNameSource m => m Builder
newName_ = do
  nm <- grabName
  return $ B.fromByteString nm
{-# INLINE newName_ #-}

grabAlias :: HasNameSource m => Builder -> m Builder
grabAlias bs = do
  nm <- grabName
  return $ bs <> B.fromChar '_' <> B.fromByteString nm
{-# INLINE grabAlias #-}

newName :: HasNameSource m => m (DList Action)
newName = liftM (D.singleton . Plain) newName_
{-# INLINE newName #-}

newNameA :: HasNameSource m => m Action
newNameA = liftM Plain newName_

newtype QueryM t a = QueryM { runQuery :: State (QueryState t) a }
        deriving (Functor, Monad, Applicative, MonadState (QueryState t))

type Query a = QueryM () a

class (HasNameSource m, Monad m) => IsQuery m where
  modifyWith :: (Maybe ExprBuilder -> Maybe ExprBuilder) -> m ()
  modifyFrom :: (Maybe ExprBuilder -> Maybe ExprBuilder) -> m ()
  modifyWhere :: (Maybe ExprBuilder -> Maybe ExprBuilder) -> m ()
  modifyRecursive :: (Bool -> Bool) -> m ()
  modifyDistinct :: (Distinct -> Distinct) -> m ()
  modifyLimit :: (Maybe Int -> Maybe Int) -> m ()
  modifyOffset :: (Maybe Int -> Maybe Int) -> m ()
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
  modifyRecursive f = do
    st <- get
    put $ st { queryStateRecursive = Any $ f (getAny $ queryStateRecursive st ) }
  modifyLimit f = do
    st <- get
    put $ st { queryStateLimit = f (queryStateLimit st) }

  modifyDistinct f = do
    st <- get
    put $ st { queryStateDistinct = f (queryStateDistinct st) }

  modifyOffset f = do
    st <- get
    put $ st { queryStateOffset = f (queryStateOffset st) }

  modifyNameSource f = do
    st <- get
    put $ st { queryNameSource = f (queryNameSource st) }
  getNameSource = do
    st <- get
    return $ queryNameSource st

  {-# INLINE modifyWith #-}
  {-# INLINE modifyFrom #-}
  {-# INLINE modifyWhere #-}
  {-# INLINE modifyRecursive #-}
  {-# INLINE modifyDistinct #-}
  {-# INLINE modifyLimit #-}
  {-# INLINE modifyOffset #-}
  {-# INLINE modifyNameSource #-}
  {-# INLINE getNameSource #-}

appendWith :: IsQuery m => ExprBuilder -> m ()
appendWith act = modifyWith $ \w ->
  (w `fappend` raw ",\n") <> Just act

{-# INLINE appendWith #-}

appendFrom :: IsQuery m => ExprBuilder -> m ()
appendFrom f = modifyFrom $ \frm ->
  (frm `fappend` raw ",\n") <> Just f
{-# INLINE appendFrom #-}

appendWhere :: IsQuery m => ExprBuilder -> m ()
appendWhere w = modifyWhere $ \wh ->
  (wh `fappend` raw " AND ") <> Just w
{-# INLINE appendWhere #-}

appendWhereExpr :: IsQuery m => Expr t -> m ()
appendWhereExpr (Expr pre w) = modifyWhere $ \wh ->
    Just $ maybe wprep (\wh' -> (wh' `D.snoc` plain " AND ") <> wprep) wh
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

finishIt :: [ExprBuilder] -> QueryState t -> ExprBuilder
finishIt expr q = finishItAgg expr Nothing q
{-# INLINE finishIt #-}

finishItAgg :: [ExprBuilder] -> Maybe (ExprBuilder) -> QueryState t -> ExprBuilder
finishItAgg expr groupBy QueryState{..} = execWriter $ do
  forM_ queryStateWith $ \w -> tell (plain withStart `D.cons` w)
  tell $ pure (plain "\nSELECT ")
  tell $ distinct queryStateDistinct
  tell $ commaSep expr
  forM_ queryStateFrom $ \f -> tell (plain "\nFROM " `D.cons` f)
  forM_ queryStateWhere $ \w -> tell (plain "\nWHERE " `D.cons` w)
  forM_ queryStateOrder $ \o -> tell (plain "\nORDER BY " `D.cons` o)
  forM_ groupBy $ \b -> do
     tell $ plain "\nGROUP BY " `D.cons` b
  forM_ queryStateLimit $ \l -> do
     tell $ D.fromList [plain "\nLIMIT ", toField l]
  forM_ queryStateOffset $ \o -> do
     tell $ D.fromList [plain "\nOFFSET ", toField o]
  where
    distinct d = case d of
      DistinctAll -> mempty
      Distinct -> raw "DISTINCT "
      DistinctOn e -> (plain "DISTINCT ON (" `D.cons` e) `D.snoc` plain ") "
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

instance FromItem (Table (Rel r)) (Rel r) where
  fromItem (Table nm) = From $ do
    let bs = T.encodeUtf8 nm
        nameBld = B.fromByteString bs
    alias <- newName_
    return $ FromTable bs alias (RelTable $ builder alias)

instance (IsRecord a) => FromItem (Query a) a where
  fromItem mq = From $ do
     (r, q) <- compIt mq
     nm <- newName
     renamed <- asRenamed r
     let bld = ((plain "(" `D.cons` finishIt (asValues r) q) `D.snoc` plain ") AS ")
               <> namedRow nm renamed
     return $ FromQuery bld renamed


fromExpr :: FromD a -> a
fromExpr (FromTable _ _ a) = a
fromExpr (FromQuery _ a) = a
fromExpr (FromInnerJoin a b _) = fromExpr a :. fromExpr b
fromExpr (FromCrossJoin a b) = fromExpr a :. fromExpr b
{-# INLINE fromExpr #-}

newtype From a = From { runFrom :: State NameSource (FromD a) }

namedRow :: IsRecord a => ExprBuilder -> a -> ExprBuilder
namedRow nm r = nm <> raw "(" <> commaSep (asValues r) <> raw ")"

instance HasNameSource (State NameSource) where
  grabNS = get
  modifyNS f = modify f
  {-# INLINE grabNS #-}
  {-# INLINE modifyNS #-}

compileFrom :: FromD a -> (ExprBuilder, a)
compileFrom (FromTable bs alias a) =
  (D.fromList [EscapeIdentifier bs, plain " AS ", Plain alias], a)
compileFrom (FromQuery bs a) = (bs, a)
compileFrom (FromInnerJoin a b cond) = (bld, ea:.eb)
  where
    (ca, ea) = compileFrom a
    (cb, eb) = compileFrom b
    bld = (ca `D.snoc` plain "\nINNER JOIN ") <> (cb `D.snoc` plain " ON ") <> cond

compileFrom (FromCrossJoin a b) = ((ca `D.snoc` plain ", ") <> cb, ea:.eb)
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
      return $ finishIt [raw "true"] q'

newtype Sorting = Sorting [ExprBuilder]
  deriving (Monoid)

sortingEmpty :: Sorting -> Bool
sortingEmpty (Sorting []) = True
sortingEmpty _ = False
{-# INLINE sortingEmpty #-}

compileSorting :: Sorting -> ExprBuilder
compileSorting (Sorting r) = mconcat $ intersperse (raw ",") r
{-# INLINE compileSorting #-}

newtype UpdExpr a = UpdExpr { getUpdates :: [(Text, ExprBuilder)] }
instance Monoid (UpdExpr a) where
  mempty = UpdExpr mempty
  UpdExpr a `mappend` UpdExpr b = UpdExpr (a<>b)

newtype Update a = Update { runUpdate :: QueryM (ExprBuilder) a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)


newtype Inserting r a = Inserting { runInserting :: QueryM [(Text, ExprBuilder)] a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)


compileInserting :: ExprBuilder -> QueryState [(Text, ExprBuilder)] -> Update ()
compileInserting table q = Update $ do
  let (cols, exprs) = unzip $ queryAction q
      withPart = queryStateWith q
      compiledSel = finishIt exprs $ q { queryStateWith = mempty }
      res = opt (raw "\nWITH " `fprepend` withPart)
            <> raw "\nINSERT INTO " <> table <> raw "("
            <> (commaSep (map (pure . escapeIdent) cols) `D.snoc` plain ") (")
            <> (compiledSel `D.snoc` plain ")")
  modify $ \st -> st { queryAction = res }

finishUpdate :: (a -> RecordParser b) -> Update a -> (ExprBuilder, RowParser b)
finishUpdate recParser (Update (QueryM u)) = (result, parser)
  where
    (expr, bld) = runState u emptyQuery
    RecordParser cols parser = recParser expr
    result = (queryAction bld `D.snoc` plain "\nRETURNING ") <> commaSep cols

compileUpdate :: (IsRecord t) => Update t -> ExprBuilder
compileUpdate (Update (QueryM u)) = (queryAction bld `D.snoc` plain "\nRETURNING ")
                                    <> (commaSep $ asValues expr)
  where
    (expr, bld) = runState u emptyQuery

finishUpdateNoRet :: Update t -> ExprBuilder
finishUpdateNoRet (Update (QueryM u)) = finish $ runState u emptyQuery
  where
    finish (_, bld) = queryAction bld


newtype Updating r a = Updating { runUpdating :: QueryM [(Text, ExprBuilder)] a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)


finishUpdating :: ExprBuilder -> QueryState [(Text, ExprBuilder)] -> ExprBuilder
finishUpdating table QueryState{..} =
         opt (raw "\nWITH " `fprepend` queryStateWith)
         <> raw "\nUPDATE " <> table <> raw "\nSET "
         <> sets
         <> opt (raw "\nFROM " `fprepend` queryStateFrom)
         <> opt (raw "\nWHERE " `fprepend` queryStateWhere)
  where
    (columns, exprs) = unzip queryAction
    sets = raw "(" <> commaSep (map (pure . escapeIdent) columns) <> raw ")=(" <> commaSep exprs <> raw ")"


compileDeleting :: ExprBuilder -> Deleting t b -> Update b
compileDeleting table (Deleting del) = Update $ do
   (r, q) <- compIt del
   let bld = opt (raw "\nWITH " `fprepend` queryStateWith q)
           <> (raw "\nDELETE FROM " <> table)
           <> opt (raw "\nUSING " `fprepend` queryStateFrom q)
           <> opt (raw "\nWHERE " `fprepend` queryStateWhere q)
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

escapeIdent :: Text -> Action
escapeIdent = EscapeIdentifier . T.encodeUtf8

call :: Function a -> Expr a
call (Function bs args) = Expr 10 $
   D.singleton (escapeIdent bs) <> raw "(" <> commaSep args <> raw ")"

callAgg :: Function t -> ExprA a
callAgg (Function bs args) = ExprAgg $
   (D.singleton $ escapeIdent bs) <> raw "(" <> commaSep args <> raw ")"

data Union = UnionDistinct | UnionAll deriving (Eq, Show, Ord)

data Recursive a = Recursive Union (Query a) (From a -> Query a)
