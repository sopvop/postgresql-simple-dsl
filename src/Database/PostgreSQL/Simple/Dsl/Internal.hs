{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}

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

import           Blaze.ByteString.Builder             (Builder)
import           Blaze.ByteString.Builder.ByteString  as B
import           Blaze.ByteString.Builder.Char8       as B

import           GHC.TypeLits                         (Sing, SingI, Symbol,
                                                       fromSing, sing)

import           Database.PostgreSQL.Simple           ((:.) (..), Only (..))
import           Database.PostgreSQL.Simple.FromField hiding (Field)
import           Database.PostgreSQL.Simple.FromRow
import           Database.PostgreSQL.Simple.ToField

data Whole a -- | Marker for selecting "whole" entity

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
       EntityParser ca rpa = entityParser (undefined :: Proxy a)
       EntityParser cb rpb = entityParser (undefined :: Proxy b)

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
addParens (RawExpr t) = (plain "(" `D.cons` t) `D.snoc` plain ")"
addParens (RawTerm t) = t

binOp :: Action -> RawExpr -> RawExpr -> Expr a
binOp op a b = Expr . RawExpr $ addParens a `D.append` (op `D.cons` addParens b)

mkAnd :: RawExpr -> RawExpr -> RawExpr
mkAnd a b = RawExpr $ addParens a `D.append` (plain " AND " `D.cons` addParens b)

mkAccess :: RawExpr -> RawExpr -> DList Action
mkAccess a b = D.append (addParens a) (plain "." `D.cons` (addParens b))



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

data CompiledFrom = FromTable Builder
                  | InnerJoin CompiledFrom CompiledFrom (Expr Bool)
                  | CrossJoin CompiledFrom Builder

data Select a = Select { selectFrom   :: ExprBuilder
                       , selectWith   :: [D.DList Action]
                       , selectWhere  :: Maybe RawExpr
                       , selectGroup  :: Maybe GroupByExpr
                       , selectOrder  :: Maybe ExprBuilder
                       , selectLimit  :: Maybe Int
                       , selectOffset :: Maybe Int
                       , selectExpr   :: a
                       }


mkSelect :: ExprBuilder -> a -> Select a
mkSelect c a = Select c [] Nothing Nothing Nothing Nothing Nothing a

-- | Source of uniques
newtype NameSource = NameSource { getNameSource :: Int }

-- | Selection query
newtype Query a = Query { runQuery :: State NameSource (Select a) }

-- | Get a unique name
grabName :: State NameSource RawExpr
grabName = do
  NameSource num <- get
  put (NameSource $ succ num)
  return . RawTerm . D.singleton . Plain $ B.fromChar 'q' <> B.fromShow num

class Projection a b | a -> b where
  toProjection :: a -> [RawExpr]
  makeSubSelects :: RawExpr -> a -> State NameSource (a, a)
    -- ^ return renamed and references to it from action

mkSubSelect :: RawExpr -> RawExpr -> Expr a
mkSubSelect s nm = Expr . RawExpr $ mkAccess s nm
mkRename :: RawExpr -> RawExpr -> Expr a
mkRename a nm = binOp (plain " AS ") a nm

instance Selectable a => Projection (Expr (Whole a)) a where
  toProjection (Expr a) = map (RawExpr . mkAccess a . mkTerm) columns
    where
      columns = entityColumns $ entityParser (undefined :: Proxy a)
  makeSubSelects t (Expr a) = do
    return $ (Expr a,  Expr t)


instance Projection (Expr (Only a)) (Only a) where
  toProjection (Expr a) = [a]
  makeSubSelects s (Expr a) = do
    nm <- grabName
    return (mkRename a nm, mkSubSelect s nm)

instance Projection (Expr a, Expr b) (a,b) where
  toProjection (Expr a, Expr b) = [a, b]
  makeSubSelects t (Expr a, Expr b) = do
    nm1 <- grabName
    nm2 <- grabName
    return ((mkRename a nm1, mkRename b nm2)
           ,(mkSubSelect t nm1, mkSubSelect t nm2))

instance Projection (Expr a, Expr b, Expr c) (a,b,c) where
  toProjection (Expr a, Expr b, Expr c) = [a, b, c]
  makeSubSelects t (Expr a, Expr b, Expr c) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3))

instance Projection (Expr a, Expr b, Expr b, Expr c) (a,b,c,d) where
  toProjection (Expr a, Expr b, Expr c, Expr d) = [a, b, c ,d]
  makeSubSelects t (Expr a, Expr b, Expr c, Expr d) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3, mkRename d nm4)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3, mkSubSelect t nm4))

instance Projection (Expr a, Expr b, Expr b, Expr c, Expr e) (a,b,c,d,e) where
  toProjection (Expr a, Expr b, Expr c, Expr d, Expr e) = [a, b, c ,d,e]
  makeSubSelects t (Expr a, Expr b, Expr c, Expr d, Expr e) = do
    nm1 <- grabName
    nm2 <- grabName
    nm3 <- grabName
    nm4 <- grabName
    nm5 <- grabName
    return ((mkRename a nm1, mkRename b nm2, mkRename c nm3, mkRename d nm4, mkRename e nm5)
           ,(mkSubSelect t nm1, mkSubSelect t nm2, mkSubSelect t nm3, mkSubSelect t nm4
                         ,mkSubSelect t nm5))

instance Projection (Expr a, Expr b, Expr b, Expr c, Expr e, Expr f) (a,b,c,d,e,f) where
  toProjection (Expr a, Expr b, Expr c, Expr d, Expr e,Expr f) = [a, b, c ,d,e,f]
  makeSubSelects t (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) = do
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

instance (Projection a c, Projection b d) => Projection (a :. b) (c:.d) where
  toProjection (a :. b) = toProjection a <> toProjection b
  makeSubSelects t (a :. b) = do
     (rena, suba) <- makeSubSelects t a
     (renb, subb) <- makeSubSelects t b
     return ((rena :. renb), (suba :. subb))


finishSelect :: Projection a b => Query a -> Action
finishSelect (Query q) = Many . D.toList $ compileSelect $ fst (runState q (NameSource 0))

compileSelect :: Projection a b => Select a -> ExprBuilder
compileSelect sel =
      withClause <> (plain "SELECT " `D.cons` proj) `D.snoc` plain " FROM "
      <> selectFrom sel <> whereClause
      <> groupByClause <> orderClause <> offsetClause <> limitClause
  where
    withClause = case selectWith sel of
      [] -> mempty
      ws -> plain "WITH " `D.cons` commaSep ws
    proj = commaSep $ map getBuilder $ toProjection (selectExpr sel)
    whereClause = case selectWhere sel of
      Nothing -> mempty
      Just ex -> plain " WHERE " `D.cons` getBuilder ex
    groupByClause = case selectGroup sel of
      Nothing -> mempty
      Just (GroupByExpr [] _) -> mempty
      Just (GroupByExpr cls havs) ->
        plain " GROUP BY " `D.cons` commaSep (map getBuilder cls)
        <> case havs of
           Nothing -> mempty
           Just havs' -> plain " HAVING " `D.cons` getBuilder havs'
    commaSep = D.concat . intersperse (D.singleton . Plain $ B.fromChar ',')
    orderClause = case selectOrder sel of
      Nothing -> mempty
      Just sorts -> plain " ORDER BY " `D.cons` sorts
    offsetClause = case selectOffset sel of
      Nothing -> mempty
      Just off -> D.fromList [plain " OFFSET ", Plain $ B.fromShow off]
    limitClause = case selectLimit sel of
      Nothing -> mempty
      Just lim -> D.fromList [plain " LIMIT ", Plain $ B.fromShow lim]


data Sorting = Asc RawExpr | Desc RawExpr

compileSorting :: Sorting -> ExprBuilder
compileSorting (Asc r) = getBuilder r `D.snoc` plain " ASC "
compileSorting (Desc r) = getBuilder r `D.snoc` plain " DESC "
