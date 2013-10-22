{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

module Database.PostgreSQL.Simple.Dsl.Internal
       where

import           Control.Applicative
import           Control.Monad.Trans.State
import           Data.ByteString                      (ByteString)
import qualified Data.ByteString.Char8                as B
import           Data.DList                           (DList)
import qualified Data.DList                           as D
import           Data.List                            (intersperse)
import           Data.Monoid
import           Data.Proxy                           (Proxy)

import           Blaze.ByteString.Builder.ByteString  as B
import           Blaze.ByteString.Builder.Char8       as B

import           GHC.TypeLits                         (Sing, SingI, Symbol,
                                                       fromSing, sing)

import           Database.PostgreSQL.Simple           ((:.) (..), Only (..))
import           Database.PostgreSQL.Simple.FromField hiding (Field)
import           Database.PostgreSQL.Simple.FromRow
import           Database.PostgreSQL.Simple.ToField

-- | Wrapper for selecting whole entity
newtype Whole a = Whole { getWhole :: a } deriving (Eq, Ord, Show)

instance Selectable a => FromRow (Whole a) where
  fromRow = fmap Whole . entityRowParser $ entityParser (Proxy :: Proxy a)

-- | Data family describing columns
data family Field v (t :: Symbol) b :: *

-- | Parser for entities with columns
data EntityParser v a = EntityParser
  { entityColumns   :: [ByteString]
  , entityRowParser :: RowParser a
  }

instance Functor (EntityParser v) where
  fmap f (EntityParser cs rp) = EntityParser cs (fmap f rp)
  {-# INLINE fmap #-}

instance Applicative (EntityParser v) where
  pure a = EntityParser mempty (pure a)
  EntityParser cs f <*> EntityParser cs' a = EntityParser (cs `mappend` cs') (f <*> a)
  {-# INLINE pure #-}
  {-# INLINE (<*>) #-}

-- | Class for naming tables
class Table v where
  tableName :: Proxy v -> ByteString

-- | Class for entities which have columns
class Selectable v where
  entityParser :: Proxy v -> EntityParser v v

instance (Selectable a, Selectable b) => Selectable (a,b) where
   entityParser _ = EntityParser (ca <> cb) ((,) <$> rpa <*> rpb)
     where
       EntityParser ca rpa = entityParser (Proxy :: Proxy a)
       EntityParser cb rpb = entityParser (Proxy :: Proxy b)

fieldSym :: SingI t => Field v t a -> Sing (t::Symbol)
fieldSym _ = sing

fieldColumn :: SingI t => Field v t a -> ByteString
fieldColumn f = B.pack . fromSing $ fieldSym f

-- | Parse named field
fromField :: (SingI f, FromField a) => (Field v f a) -> EntityParser v a
fromField f = EntityParser ([fieldColumn f]) field

type ExprBuilder = DList Action

data RawExpr = RawTerm ExprBuilder
             | RawExpr ExprBuilder

getBuilder :: RawExpr -> ExprBuilder
getBuilder (RawTerm t) = t
getBuilder (RawExpr t) = t

instance ToField RawExpr where
  toField (RawTerm e) = Many $ D.toList e
  toField (RawExpr e) = Many $ D.toList e

-- | Adds phantom type to RawExpr
newtype Expr a = Expr { getRawExpr :: RawExpr }

instance ToField (Expr a) where
  toField (Expr a) = toField a


plain :: ByteString -> Action
plain = Plain . B.fromByteString
mkTerm :: ByteString -> RawExpr
mkTerm = RawTerm . D.singleton . plain
addParens :: RawExpr -> DList Action
addParens (RawExpr t) = raw "(" <> t <> raw ")"
addParens (RawTerm t) = t

opt Nothing = mempty
opt (Just x) = x

fprepend :: (Functor f, Monoid b) => b -> f b -> f b
fprepend p a = (<> p) <$> a
fappend :: (Functor f, Monoid b) => f b -> b -> f b
fappend a x = (x <>) <$> a

raw :: ByteString -> ExprBuilder
raw = D.singleton . plain

rawField :: ToField a => a -> ExprBuilder
rawField = D.singleton . toField

binOp :: Action -> RawExpr -> RawExpr -> RawExpr
binOp op a b = RawExpr $ addParens a <> D.singleton op <> addParens b

binOpE :: Action -> RawExpr -> RawExpr -> Expr a
binOpE op a b = Expr (binOp op a b)

mkAnd :: RawExpr -> RawExpr -> RawExpr
mkAnd a b = binOp (plain " AND ") a b

mkAccess :: RawExpr -> RawExpr -> DList Action
mkAccess a b = addParens a <> raw "." <> addParens b

commaSep :: [ExprBuilder] -> ExprBuilder
commaSep = D.concat . intersperse (D.singleton . Plain $ B.fromChar ',')

type Namer a = State NameSource a

data GroupByExpr = GroupByExpr
     { groupByCols    :: [RawExpr]
     , groupByHavings :: Maybe RawExpr
     }

instance Monoid GroupByExpr where
  mempty = GroupByExpr [] Nothing
  GroupByExpr as ah `mappend` GroupByExpr bs bh =
    GroupByExpr (as <> bs) havs
    where
      havs = case ah of
        Nothing -> bh
        Just ah'-> case bh of
          Nothing -> Just ah'
          Just bh' -> Just $ mkAnd ah' bh'

data Selector a = Selector { selectFrom  :: Maybe ExprBuilder
                           , selectWith  :: [ExprBuilder]
                           , selectWhere :: Maybe RawExpr
                           , selectExpr  :: AsExpr a
                           }

mkSelector :: ExprBuilder -> (AsExpr a) -> Selector a
mkSelector c a = Selector (Just c) [] Nothing a

mkSelector' :: Maybe ExprBuilder -> (AsExpr a) -> Selector a
mkSelector' c a = Selector c [] Nothing a

-- | Source of uniques
newtype NameSource = NameSource { getNameSource :: Int }

-- | Select query
newtype Select a = Select { runSelect :: State NameSource (Selector a) }

newtype Query a = Query { runQuery :: State NameSource (Finishing a) }

-- | Get a unique name
grabName :: State NameSource RawExpr
grabName = do
  NameSource num <- get
  put (NameSource $ succ num)
  return . RawTerm . D.singleton . Plain $ B.fromChar 'q' <> B.fromShow num


type family FromExpr a :: *
type instance FromExpr (Expr (Whole a)) = Whole a
type instance FromExpr (Expr (Only a)) = Only a
type instance FromExpr (Expr a, Expr b) = (a,b)
type instance FromExpr (Expr a, Expr b, Expr c) = (a,b,c)
type instance FromExpr (Expr a, Expr b, Expr c, Expr d) = (a,b,c,d)
type instance FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e) = (a,b,c,d,e)
type instance FromExpr (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = (a,b,c,d,e,f)
type instance FromExpr (a :. b) = (FromExpr a :. FromExpr b)


class (FromExpr (AsExpr a) ~ a) => IsExpr a where
  type AsExpr a :: *
  compileProjection :: AsExpr a -> [RawExpr]
  makeSubRename :: RawExpr -> AsExpr a -> State NameSource (AsExpr a, AsExpr a)

instance Selectable a => IsExpr (Whole a) where
  type AsExpr (Whole a) = Expr (Whole a)
  compileProjection (Expr a) = map (RawExpr . mkAccess a . mkTerm) columns
    where
      columns = entityColumns $ entityParser (Proxy :: Proxy a)
  makeSubRename t (Expr a) = do
    return $ (Expr a,  Expr t)


instance IsExpr (Only a) where
  type AsExpr (Only a) = Expr (Only a)
  compileProjection (Expr a) = [a]
  makeSubRename s (Expr a) = do
    nm <- grabName
    return (mkRename a nm, mkSubSelect s nm)

instance IsExpr (a,b) where
  type AsExpr (a,b) = (Expr a, Expr b)
  compileProjection (Expr a, Expr b) = [a, b]
  makeSubRename t (Expr a, Expr b) = do
    nm1 <- grabName
    nm2 <- grabName
    return ((mkRename a nm1, mkRename b nm2)
           ,(mkSubSelect t nm1, mkSubSelect t nm2))

instance IsExpr (a,b,c) where
  type AsExpr (a,b,c) = (Expr a, Expr b, Expr c)
  compileProjection (Expr a, Expr b, Expr c) = [a, b, c]
  makeSubRename t (Expr a, Expr b, Expr c) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3))

instance IsExpr (a,b,c,d) where
  type AsExpr (a,b,c,d) = (Expr a, Expr b, Expr c, Expr d)
  compileProjection (Expr a, Expr b, Expr c, Expr d) = [a, b, c ,d]
  makeSubRename t (Expr a, Expr b, Expr c, Expr d) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3, mkRename d nm4)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3, mkSubSelect t nm4))

instance IsExpr (a,b,c,d,e) where
  type AsExpr (a,b,c,d,e) = (Expr a, Expr b, Expr c, Expr d, Expr e)
  compileProjection (Expr a, Expr b, Expr c, Expr d, Expr e) = [a, b, c, d,e]
  makeSubRename t (Expr a, Expr b, Expr c, Expr d, Expr e) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    nm5 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3, mkRename d nm4, mkRename e nm5)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3, mkSubSelect t nm4
                         ,mkSubSelect t nm5))

instance IsExpr (a,b,c,d,e,f) where
  type AsExpr (a,b,c,d,e,f) = (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
  compileProjection (Expr a, Expr b, Expr c, Expr d, Expr e,Expr f) = [a, b, c ,d,e,f]
  makeSubRename t (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    nm5 <- grabName
    nm6 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3, mkRename d nm4, mkRename e nm5
                      ,mkRename f nm6)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3, mkSubSelect t nm4
                         ,mkSubSelect t nm5, mkSubSelect t nm6))

instance (IsExpr a, IsExpr b) => IsExpr (a:.b) where
  type AsExpr (a :. b) = (AsExpr a :. AsExpr b)
  compileProjection (a :. b) = compileProjection a <> compileProjection b
  makeSubRename t (a :. b) = do
     (rena, suba) <- makeSubRename t a
     (renb, subb) <- makeSubRename t b
     return ((rena :. renb), (suba :. subb))


mkSubSelect :: RawExpr -> RawExpr -> Expr a
mkSubSelect s nm = Expr . RawExpr $ mkAccess s nm
mkRename :: RawExpr -> RawExpr -> Expr a
mkRename a nm = binOpE (plain " AS ") a nm


finishSelect :: IsExpr a => Select a -> Action
finishSelect (Select mq) = Many . D.toList . fst . fst $
  flip runState (NameSource 0) $ do
    nm <- grabName
    sel <- mq
    compileSelector sel nm

prepareSelector :: (IsExpr a) => Selector a -> RawExpr ->
                Namer (ExprBuilder, Maybe ExprBuilder, ExprBuilder, ExprBuilder, AsExpr a)
prepareSelector sel nm = do
  (projExprs, access) <- makeSubRename nm $ selectExpr sel
  let withClause = case selectWith sel of
        [] -> mempty
        ws -> raw "WITH " <> commaSep ws
      proj = commaSep $ map getBuilder $ compileProjection projExprs
      whereClause = (mappend (raw " WHERE ") . getBuilder) <$> selectWhere sel
  return (withClause, selectFrom sel, opt whereClause, proj, access)

compileSelector :: IsExpr a => Selector a -> RawExpr -> Namer (ExprBuilder, AsExpr a)
compileSelector sel nm = do
  (with, from, where', project, access) <- prepareSelector sel nm
  return (with <> raw "SELECT " <> project <>
         opt (raw " FROM " `fprepend` from)
         <> where', access)

data Finishing a = Finishing
  { finishingSelect :: ExprBuilder
  , finishingGroup  :: Maybe (AsExpr a -> ExprBuilder)
  , finishingHaving :: Maybe (AsExpr a -> ExprBuilder)
  , finishingOrder  :: Maybe (AsExpr a -> ExprBuilder)
  , finishingLimit  :: Maybe Int
  , finishingOffset :: Maybe Int
  , finishingExpr   :: AsExpr a
  }

mkFinishing :: ExprBuilder -> AsExpr a -> Finishing a
mkFinishing bld a = Finishing bld Nothing Nothing Nothing Nothing Nothing a

finishQuery :: (IsExpr a) =>  Query a -> Action
finishQuery (Query q) = Many . D.toList . fst  $
             runState (compileFinishing q) (NameSource 0)

compileFinishing :: IsExpr a => Namer (Finishing a) -> Namer (ExprBuilder)
compileFinishing mfin = do
  fin <- mfin
  let exprs = finishingExpr fin
      having = Just (raw " HAVING ") <> (finishingHaving fin <*> pure exprs)
      groupByClause = case finishingGroup fin of
        Nothing -> mempty
        Just order ->
          raw " GROUP BY " <> order exprs <> opt having
      orderClause = raw " ORDER BY " `fprepend` (finishingOrder fin <*> pure exprs)
      offsetClause= raw " OFFSET " `fprepend` (rawField <$> finishingOffset fin)
      limitClause = raw " LIMIT " `fprepend` (rawField <$> finishingLimit fin)
  return $ finishingSelect fin
         <> opt orderClause
         <> groupByClause
         <> opt offsetClause
         <> opt limitClause

data Sorting = Asc RawExpr | Desc RawExpr

compileSorting :: Sorting -> ExprBuilder
compileSorting (Asc r) = getBuilder r <> raw " ASC "
compileSorting (Desc r) = getBuilder r <> raw " DESC "

data UpdatingAct = DoUpdate [(ByteString, RawExpr)]
                 | DoInsert [(ByteString, RawExpr)]
                 | DoInsertMany [ByteString] [RawExpr]
                 | DoDelete

data Updating a = Updating
  { updatingSelect :: Selector a
  , updatingAct    :: UpdatingAct
  , updatingTable  :: ExprBuilder
  }

newtype Update a = Update { runUpdate :: State NameSource (Updating a) }


compileUpdateAct :: IsExpr a => Selector a -> [(ByteString, RawExpr)] -> ExprBuilder
              -> Namer (ExprBuilder, ExprBuilder, AsExpr a)
compileUpdateAct sel upd tbl = do
  nm <- grabName
  (with, from, where', project, access) <- prepareSelector sel nm
  let setClause = commaSep $ map mkUpd upd
      updTable = raw " UPDATE " <> tbl <> raw " SET "
      ret = case D.toList project of
        [] -> mempty
        xs -> D.fromList (plain " RETURNING " : xs)
      from' = raw " FROM " `fprepend` from
  return $ (with <> updTable <> setClause <> opt from' <> where', ret, access)
  where
    mkUpd (bs, act) = D.singleton (Plain $ B.fromByteString bs <> B.fromByteString "=")
                          <> getBuilder act

compileInsertAct :: IsExpr a => Selector a -> [(ByteString, RawExpr)] -> ExprBuilder
              -> Namer (ExprBuilder, ExprBuilder, AsExpr a)
compileInsertAct sel upd tbl = do
  nm <- grabName
  (with, from, where', project, access) <- prepareSelector sel nm
  let cols = commaSep $ map (D.singleton.plain.fst) upd
      vals = commaSep $ map (getBuilder.snd) upd
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
  (with, from, where', project, access) <- prepareSelector sel nm
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

newtype UpdExpr a = UpdExpr { getUpdates :: [(ByteString, RawExpr)] }
instance Monoid (UpdExpr a) where
  mempty = UpdExpr mempty
  UpdExpr a `mappend` UpdExpr b = UpdExpr (a<>b)

