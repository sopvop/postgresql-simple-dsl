{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PolyKinds                  #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

module Database.PostgreSQL.Simple.Dsl.Internal
       where

import           Control.Monad.State.Class
import           Control.Monad.Trans.Reader              (ReaderT, runReaderT)
import           Control.Monad.Trans.State
    (State, evalState, execState, runState)
import           Control.Monad.Trans.Writer

import           Data.ByteString.Builder
    (Builder, char8, lazyByteString, toLazyByteString)
import qualified Data.ByteString.Lazy                    as Lazy
import Data.Kind (Type)
import           Data.Coerce
import           Data.List                               (intersperse)
import           Data.Semigroup
    (Any (..))
import Data.Foldable(for_)

import           Data.Text                               (Text)
import qualified Data.Text.Encoding                      as Text

import           Data.HashMap.Strict                     (HashMap)
import qualified Data.HashMap.Strict                     as HashMap


import           Database.PostgreSQL.Simple              ((:.) (..), Only (..))
import           Database.PostgreSQL.Simple.FromRow      as PG
import           Database.PostgreSQL.Simple.ToField

import           Database.PostgreSQL.Simple.Dsl.Escaping
import           Database.PostgreSQL.Simple.Dsl.Lens
import           Database.PostgreSQL.Simple.Dsl.Types

type LazyByteString = Lazy.ByteString

term :: RawExpr -> Expr a
term = Expr 0

parenPrec :: Bool -> RawExpr -> RawExpr
parenPrec True bld = addParens bld
parenPrec  _    bld = bld


addParens :: RawExpr -> RawExpr
addParens t = char8 '(' <> t <> char8 ')'

opt :: Monoid a => Maybe a -> a
opt Nothing = mempty
opt (Just x) = x

fprepend :: (Functor f, Monoid b) => b -> f b -> f b
fprepend p a = (p <>) <$> a
fappend :: (Functor f, Monoid b) => f b -> b -> f b
fappend a x = (<> x) <$> a

rawField :: ToField a => a -> RawExpr
rawField = escapeAction . toField


binOp :: IsExpr expr => Int -> RawExpr -> expr a -> expr b -> expr c
binOp p op ea eb = fromExpr . Expr p $
   parenPrec (p <= pa) a <> op <> parenPrec (p < pb) b
  where
    (Expr pa a) = toExpr ea
    (Expr pb b) = toExpr eb
{-# INLINE binOp #-}

prefOp :: IsExpr expr => Int -> RawExpr -> expr a -> expr b
prefOp p op ea = fromExpr . Expr p $ op <> parenPrec (p < pa) a
  where
    (Expr pa a) = toExpr ea
{-# INLINE prefOp #-}

commaSep :: [RawExpr] -> RawExpr
commaSep = mconcat . intersperse (char8 ',')

-- | Source of uniques

asRenamed :: forall a m . (IsRecord a, HasNameSource m) => a -> m a
asRenamed _ = genNames


justNullable :: Nullable a -> a
justNullable = getNullable

fromNullable :: NulledRecord a b => Nullable a -> b
fromNullable = nulled . getNullable

class NulledRecord a b | a -> b where
  nulled :: a -> b

instance n ~ Nulled a => NulledRecord (Expr a) (Expr n) where
  nulled = coerce
  {-# INLINE nulled #-}

instance (n1 ~ Nulled e1, n2 ~ Nulled e2) =>
   NulledRecord (Expr e1, Expr e2) (Expr n1, Expr n2) where
  nulled = coerce
  {-# INLINE nulled #-}

instance (n1 ~ Nulled e1, n2 ~ Nulled e2, n3 ~ Nulled e3) =>
   NulledRecord (Expr e1, Expr e2, Expr e3) (Expr n1, Expr n2, Expr n3) where
  nulled = coerce
  {-# INLINE nulled #-}

instance (NulledRecord a na , NulledRecord b nb)
   => NulledRecord ( a :. b) (na :. nb) where
  nulled (a :. b) = nulled a :. nulled b
  {-# INLINE nulled #-}


class ToRecord a where
  type AsRecord a :: Type
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
  type AggRecord a :: Type
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



data Distinct = DistinctAll | Distinct | DistinctOn Sorting

data QueryState t = QueryState
  { queryStateWith      :: Maybe RawExpr
  , queryStateDistinct  :: Distinct
  , queryStateRecursive :: Any
  , queryStateFrom      :: Maybe RawExpr
  , queryStateWhere     :: Maybe RawExpr
  , queryStateOrder     :: Maybe Sorting
  , queryStateGroupBy   :: Maybe RawExpr
  , queryStateHaving    :: Maybe RawExpr
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


newtype QueryM t a = QueryM { runQuery :: State (QueryState t) a }
        deriving (Functor, Monad, Applicative, MonadState (QueryState t))

type Query a = QueryM () a

class (HasNameSource m, Monad m) => IsQuery m where
  modifyWith :: (Maybe RawExpr -> Maybe RawExpr) -> m ()
  modifyFrom :: (Maybe RawExpr -> Maybe RawExpr) -> m ()
  modifyWhere :: (Maybe RawExpr -> Maybe RawExpr) -> m ()
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

appendWith :: IsQuery m => RawExpr -> m ()
appendWith act = modifyWith $ \w ->
  (w `fappend` ",\n") <> Just act

{-# INLINE appendWith #-}

appendFrom :: IsQuery m => RawExpr -> m ()
appendFrom f = modifyFrom $ \frm ->
  (frm `fappend` ",\n") <> Just f
{-# INLINE appendFrom #-}

appendWhere :: IsQuery m => RawExpr -> m ()
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

appendHaving :: RawExpr -> Aggregator ()
appendHaving e = Aggregator . modify $ \st ->
      st { queryStateHaving = (queryStateHaving st `fappend` " AND ") <> Just e }
{-# INLINE appendHaving #-}

appendGroupBy :: RawExpr -> Aggregator ()
appendGroupBy e = Aggregator . modify $ \st ->
   st { queryStateGroupBy = (queryStateGroupBy st `fappend` char8 ',') <> Just e }

{-# INLINE appendGroupBy #-}

finishIt :: [RawExpr] -> QueryState t -> RawExpr
finishIt expr QueryState{..} = execWriter $ do
  for_ queryStateWith $ \w -> tell (withStart <> w)
  tell $ ("\nSELECT ")
  tell $ distinct queryStateDistinct
  tell $ commaSep expr
  for_ queryStateFrom $ \f -> tell ("\nFROM " <> f)
  for_ queryStateWhere $ \w -> tell ("\nWHERE " <> w)
  for_ queryStateGroupBy $ \b -> do
     tell $ "\nGROUP BY " <> b
  for_ queryStateHaving $ \b -> do
     tell $ "\nHAVING " <> b
  for_ order $ \o -> tell ("\nORDER BY " <> compileSorting o)
  for_ queryStateLimit $ \l -> do
     tell $ "\nLIMIT " <> escapeAction (toField l)
  for_ queryStateOffset $ \o -> do
     tell $ "\nOFFSET " <> escapeAction (toField o)
  where
    distinct d = case d of
      DistinctAll -> mempty
      Distinct -> "DISTINCT "
      DistinctOn (Sorting e) -> "DISTINCT ON (" <> (mconcat $ intersperse "," $ map fst e)
                 <>  ") "
    distinctOrder = case queryStateDistinct of
        DistinctOn es -> Just es
        _ -> Nothing

    order = distinctOrder <> queryStateOrder
    withStart = if getAny queryStateRecursive then "\nWITH RECURSIVE "
                 else "\nWITH "

compileQuery :: forall a t . (IsRecord a, Monoid t) => QueryM t a -> RawExpr
compileQuery q = evalState comp (emptyQuery :: QueryState t)
 where
   comp = runQuery $ do
     (r,q') <- compIt q
     return $ finishIt (asValues r) q'

instance {-# OVERLAPPABLE #-}  ToColumns a => FromItem (Function a) a where
  fromItem (Function nm args) = From $ do
     alias <- grabName --Alias bs nameBld
     let r = toColumns (alias <> char8 '.')
     pure (escapeIdent nm <> char8 '(' <> commaSep args <> ") AS " <> alias, r)

instance {-# OVERLAPPING #-} FromItem (Function (Expr a)) (Expr a) where
  fromItem (Function nm args) = From $ do
     alias <- grabName --Alias bs nameBld
     pure (escapeIdent nm <> char8 '(' <> commaSep args <> ") AS " <> alias, Expr 0 alias)

instance (IsRecord a) => FromItem (Query a) a where
  fromItem mq = From $ do
     (r, q) <- compIt mq
     nm <- grabName
     renamed <- asRenamed r
     let bld = char8 '(' <> finishIt (asValues r) q <> ") AS "
               <> namedRow nm renamed
     return $ (bld, renamed)

data Table a = Table !Text

instance ToColumns a => FromItem (Table a) a where
   fromItem (Table nm) = From $ do
      alias <- grabName
      let
        bld = escapeIdentifier $ Text.encodeUtf8 nm
        r = toColumns (alias <> char8 '.')
        q = bld <> " AS " <> alias
      pure (q, r)


namedRow :: IsRecord a => RawExpr -> a -> RawExpr
namedRow nm r = nm <> char8 '(' <> commaSep (asValues r) <> char8 ')'

newtype Lateral a = Lateral (Query a)

instance (IsRecord a) => FromItem (Lateral a) a where
  fromItem (Lateral mq) = From $ do
     (r, q) <- compIt mq
     nm <- grabName
     renamed <- asRenamed r
     let bld = " LATERAL (" <> finishIt (asValues r) q <> ") AS "
               <> namedRow nm renamed
     return $ (bld, renamed)

finishQuery :: (a -> RecordParser b) -> Query a -> (RawExpr, RowParser b)
finishQuery fromRec mq = evalState finisher $ (emptyQuery :: QueryState ())
  where
    finisher = runQuery $ do (r,q') <- compIt mq
                             let RecordParser cols parser = fromRec r
                             return (finishIt cols q', parser)

finishQueryNoRet :: Query t -> RawExpr
finishQueryNoRet mq = evalState finisher (emptyQuery :: QueryState ())
  where
    finisher = runQuery $ do
      (_, q') <- compIt mq
      return $ finishIt ["true"] q'

newtype Sorting = Sorting [(RawExpr, RawExpr)]
  deriving (Semigroup, Monoid)

sortingEmpty :: Sorting -> Bool
sortingEmpty (Sorting []) = True
sortingEmpty _ = False
{-# INLINE sortingEmpty #-}

compileSorting :: Sorting -> RawExpr
compileSorting (Sorting r) = mconcat . intersperse (char8 ',')
               . map (\(expr, order) -> (expr <> order)) $ r
{-# INLINE compileSorting #-}

newtype UpdExpr a = UpdExpr { getUpdates :: HashMap LazyByteString RawExpr }

instance Semigroup (UpdExpr a) where
  UpdExpr a <> UpdExpr b = UpdExpr (a<>b)

instance Monoid (UpdExpr a) where
  mempty = UpdExpr mempty

newtype Update a = Update { runUpdate :: QueryM RawExpr a }
   deriving (Functor, Applicative, Monad, HasNameSource)

newtype Inserting r a = Inserting { runInserting :: QueryM (HashMap LazyByteString RawExpr) a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)


compileInserting :: RawExpr -> QueryState [(LazyByteString, RawExpr)] -> Update ()
compileInserting table q = Update $ do
  let (cols, exprs) = unzip $ queryAction q
      withPart = queryStateWith q
      compiledSel = finishIt exprs $ q { queryStateWith = mempty }
      res = opt ("\nWITH " `fprepend` withPart)
            <> "\nINSERT INTO " <> table <> char8 '('
            <> (commaSep (map lazyByteString cols) <> ") (")
            <> (compiledSel <> ")")
  modify $ \st -> st { queryAction = res }

finishInserting :: RawExpr -> QueryState (HashMap LazyByteString RawExpr) -> RawExpr
finishInserting table q@QueryState{..} =
    opt ("\nWITH " `fprepend` queryStateWith)
      <> "\nINSERT INTO " <> table <> char8 '('
      <> (commaSep (map lazyByteString columns) <> ") (")
      <> (compiledSel <> ")")
  where
    compiledSel = finishIt exprs $ q { queryStateWith = mempty }
    (columns, exprs) = unzip $ HashMap.toList queryAction

compileInserting' :: RawExpr -> Inserting t a -> QueryM RawExpr a
compileInserting' table (Inserting a) = do
  (r, q) <- compIt a
  let res = finishInserting table q
  modify $ \st -> st { queryAction = res }
  return r


finishUpdate :: (a -> RecordParser b) -> Update a -> (RawExpr, RowParser b)
finishUpdate recParser (Update (QueryM u)) = (result, parser)
  where
    (expr, bld) = runState u emptyQuery
    RecordParser cols parser = recParser expr
    result = queryAction bld <> "\nRETURNING " <> commaSep cols

compileUpdate :: (IsRecord t) => Update t -> RawExpr
compileUpdate (Update (QueryM u)) =
    queryAction bld <> "\nRETURNING " <> commaSep (asValues expr)
  where
    (expr, bld) = runState u emptyQuery

finishUpdateNoRet :: Update t -> RawExpr
finishUpdateNoRet (Update (QueryM u)) = finish $ runState u emptyQuery
  where
    finish (_, bld) = queryAction bld


newtype Updating r a = Updating { runUpdating :: QueryM (HashMap LazyByteString RawExpr) a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)


finishUpdating :: RawExpr -> QueryState (HashMap LazyByteString RawExpr) -> RawExpr
finishUpdating table QueryState{..} =
         opt ("\nWITH " `fprepend` queryStateWith)
         <> "\nUPDATE " <> table <> "\nSET "
         <> sets
         <> opt ("\nFROM " `fprepend` queryStateFrom)
         <> opt ("\nWHERE " `fprepend` queryStateWhere)
  where
    sets = mconcat
         . intersperse (char8 ',')
         . fmap go
         $ HashMap.toList queryAction
    go (column, value) = lazyByteString column <> "=" <> value


compileDeleting :: RawExpr -> Deleting t b -> Update b
compileDeleting table (Deleting del) = Update $ do
   (r, q) <- compIt del
   let bld = opt ("\nWITH " `fprepend` queryStateWith q)
           <> ("\nDELETE FROM " <> table)
           <> opt ("\nUSING " `fprepend` queryStateFrom q)
           <> opt ("\nWHERE " `fprepend` queryStateWhere q)
   modify $ \st -> st { queryAction = bld }
   return r


compileUpdating :: RawExpr -> Updating t a -> QueryM RawExpr a
compileUpdating table (Updating a) = do
  (r, q) <- compIt a
  let res = finishUpdating table q
  modify $ \st -> st { queryAction = res }
  return r

newtype Deleting t a = Deleting { runDeleting :: QueryM () a }
   deriving (Functor, Applicative, Monad, IsQuery, HasNameSource)

data Function a = Function { functionName :: Text
                           , functionArgs :: [RawExpr]
                           }

arg :: Expr t -> Function a -> Function a
arg (Expr _ v) f = f { functionArgs = functionArgs f ++ [v]}

function :: Text -> Function a
function bs = Function bs []

escapeIdent :: Text -> Builder
escapeIdent = escapeIdentifier . Text.encodeUtf8

call :: Function a -> Expr a
call (Function bs args) = Expr 10 $
     escapeIdent bs <> char8 '(' <> commaSep args <> char8 ')'

callAgg :: Function t -> ExprA a
callAgg (Function bs args) = ExprA . Expr 0 $
   escapeIdent bs <> char8 '(' <> commaSep args <> char8 ')'

data Union = UnionDistinct | UnionAll deriving (Eq, Show, Ord)

data Recursive a = Recursive Union (Query a) (From a -> Query a)


update :: forall a b . ToColumns a => Table a -> (a -> Updating a b) -> Update b
update (Table nm) f = Update $ do
  alias <- grabName
  let r = toColumns (alias <> char8 '.')
      q = nameBld <> " AS " <> alias
      upd = f r
  compileUpdating q upd
  where
      nameBld = escapeIdentifier $ Text.encodeUtf8 nm


delete :: forall a  b . ToColumns a => Table a -> (a -> Deleting a b) -> Update b
delete (Table nm) f = do
  alias <- grabName
  let r = toColumns (alias <> char8 '.')
      q = nameBld <> " AS " <> alias
      upd = f r
  compileDeleting q upd
  where
     nameBld = escapeIdentifier $ Text.encodeUtf8 nm


insert :: forall a . ToColumns a => Table a -> Query (UpdExpr a) -> Update a
insert (Table nm) mq = do
  let r = toColumns mempty
  (UpdExpr upd, q) <- compIt mq
  compileInserting (escapeIdent nm) $ q {queryAction = HashMap.toList upd}
  return r

insert' :: forall a b . ToColumns a => Table a -> Inserting a b -> Update a
insert' (Table nm) upd = Update $ do
  let r = toColumns mempty
  _ <- compileInserting' nameBld upd
  return r
  where
      nameBld = escapeIdentifier $ Text.encodeUtf8 nm




newtype UpdaterM t a = UpdaterM (ReaderT t (State (HashMap.HashMap Lazy.ByteString RawExpr)) a)

instance Functor (UpdaterM t) where
  fmap f (UpdaterM !a) = UpdaterM $! (fmap f a)

instance Applicative (UpdaterM t) where
  pure = UpdaterM . pure
  UpdaterM f <*> UpdaterM v = UpdaterM $! f <*> v

instance Monad (UpdaterM t) where
  return = pure
  UpdaterM mv >>= mf = UpdaterM $ do
    v <- mv
    let UpdaterM f = mf v
    f

type Updater t = UpdaterM t ()


mkUpdate :: ToColumns a => Updater a -> UpdExpr a
mkUpdate (UpdaterM u) = UpdExpr (execState (runReaderT u table ) mempty)
  where
    table = toColumns mempty


setField :: Lens' s (Expr a) -> Expr a -> Updater s
setField l (Expr _ a) = UpdaterM $ do
   Expr _ nm <- views l
   modify $ HashMap.insert (toLazyByteString nm) a

setFieldVal :: ToField a => Lens' s (Expr a) -> a -> Updater s
setFieldVal l a = setField l (term $ rawField a)

infixr 7 =., =.!
(=.) :: Lens' s (Expr a) -> Expr a -> Updater s
(=.) = setField

(=.!) :: ToField a => Lens' s (Expr a) -> a -> Updater s
(=.!) = setFieldVal
