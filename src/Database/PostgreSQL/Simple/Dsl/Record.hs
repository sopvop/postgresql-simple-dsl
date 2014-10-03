{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE OverlappingInstances #-}

module Database.PostgreSQL.Simple.Dsl.Record
  ( (:::)(..)
  , Rec(..)
  , (=:)
  , (<+>)
  , rhead
  , rtail
  , Elem
  , IElem
  , rLens
  , rGet
  , rSet
  , rOver
  , rupdate
  , rinsert
  , rdelete
  , setR
  , (.>)
  , Updater
  , UpdaterM
  , mkUpdate
  , setU
  , setVal
  ) where

import           GHC.TypeLits

import           Control.Applicative
import           Data.Functor.Identity
import           Data.Monoid
import           Data.Foldable
import           Control.Monad.Trans.State.Strict        (State, execState, modify)
import qualified Data.HashMap.Strict                     as HashMap

import           Data.Proxy
import qualified Data.DList as D
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           Database.PostgreSQL.Simple.FromField
import           Database.PostgreSQL.Simple.ToField
import           Blaze.ByteString.Builder                as B
import           Blaze.ByteString.Builder.Char8          as B
import           Database.PostgreSQL.Simple.Dsl.Internal


-- | Field with name
data k ::: s where
  SField :: KnownSymbol sy => (sy ::: t)

-- | Record
data Rec a where
  RNil :: Rec '[]
  (:&) :: KnownSymbol k => !a -> !(Rec rs) -> Rec ((k ::: a) ': rs)
infixr :&

instance Show (Rec '[]) where
  showsPrec p RNil | p == 10 = showString "]"
                   | otherwise = showString "[]"
instance (KnownSymbol t, Show a, Show (Rec b)) => Show (Rec ((t ::: a) ': b)) where
  showsPrec p (a :& b)
     | p == 10 = showString ", " . showString (symbolVal (Proxy :: Proxy t))
                 . showString " = " . showsPrec 11 a . showsPrec 10 b
     | otherwise = showString "[" . showString (symbolVal (Proxy :: Proxy t))
                 .  showString " = " . showsPrec 11 a . showsPrec 10 b

-- | Record constuctor from field and value
(=:) :: KnownSymbol k => (k ::: a) -> a -> Rec '[ k ::: a ]
_ =: x = x :& RNil
{-# INLINE (=:) #-}
-- | Append recrods
(<+>) :: Rec as -> Rec bs -> Rec as +++ Rec bs
RNil <+> r = r
(x :& ls) <+> rs = x :& (ls <+> rs)
infixr 5 <+>
{-# INLINE (<+>) #-}

-- | Type level append
type family (as :: [k]) ++ (bs :: [k]) :: [k]
type instance '[] ++ bs = bs
type instance (a ': as) ++ bs = a ': (as ++ bs)

-- | Record append
type family a +++ b :: c where
  Rec as +++ Rec bs = Rec (as ++ bs)

-- | head for records
rhead :: Rec ((k ::: a) ': b) -> a
rhead ( x :& _) = x
{-# INLINE rhead #-}

-- | tail for recods
rtail :: Rec ((t ::: a) ': b) -> Rec b
rtail ( _ :& r) = r
{-# INLINE rtail #-}

instance KnownSymbol t => IsRecord (Rec '[t ::: Expr a]) where
  asRenamed _ = do
    n <- newExpr
    return $ SField =: n
  asValues = pure . getRawExpr . rhead

instance (KnownSymbol t, IsRecord (Rec (b ': c))) =>
         IsRecord (Rec ((t ::: Expr a) ': (b ': c))) where
  asRenamed expr = do
    n <- newExpr
    res <- asRenamed $ rtail expr
    return $ SField =: n <+> res
  asValues (a :& as) = getRawExpr a : asValues as

instance FromField a => FromRecord (Rec '[t ::: Expr a]) (Rec '[t ::: a]) where
  fromRecord (r :& _) = (SField =:) <$> recField r

instance (FromRecord (Rec (b ': c)) (Rec (b' ': c')), FromField a) =>
   FromRecord (Rec ((t ::: Expr a) ': (b ': c)))
   (Rec ((t ::: a) ': (b' ': c'))) where
  fromRecord (a :& as) = (\a' b' -> SField =: a' <+> b') <$> recField a
                                                         <*> fromRecord as

-- | Element location witness
data Elem :: k -> [k] -> * where
  Here :: Elem x (x ': xs)
  There :: Elem x xs -> Elem x (y ': xs)

class Implicit p where
  implicitly :: p

instance Implicit (Elem x (x ': xs)) where
  implicitly = Here

instance Implicit (Elem x xs) => Implicit (Elem x (y ': xs)) where
  implicitly = There implicitly

type IElem x xs = Implicit (Elem x xs)

{-
class (xs :: [*]) <: (ys :: [*]) where
  cast :: Rec xs -> Rec ys

instance xs <: '[] where
  cast _ = RNil

instance (y ~ (t ::: a), IElem y xs) => xs <: (y ': ys) where
  cast xs = ith (implicitly :: Elem y xs) xs <+> cast xs
    where
      ith :: Elem y rs -> Rec rs -> Rec (y ': '[])
      ith Here (a :& _) = SField =: a
      ith (There p) (_ :& as) = ith p as
-}
-- | Make a Lens' from field
rLens :: forall t a rs r g .(Functor g, r ~ (t ::: a), IElem r rs) =>
       r -> (a -> g a) -> Rec rs -> g (Rec rs)
rLens _ f = go implicitly
  where
    go :: Elem r rr -> Rec rr -> g (Rec rr)
    go Here (x :& xs) = (:& xs) <$> f x

    go (There Here) (a :& x :& xs) = fmap ((a :&) . (:& xs)) (f x)

    go (There (There Here)) (a :& b :& x :& xs) =
      fmap (\x' -> a :& b :& x' :& xs) (f x)
    go (There (There (There Here))) (a :& b :& c :& x :& xs) =
      fmap (\x' -> a :& b :& c :& x' :& xs) (f x)
    go (There (There (There (There Here)))) (a :& b :& c :& d :& x :& xs) =
      fmap (\x' -> a :& b :& c :& d :& x' :& xs) (f x)

    go (There (There (There (There p)))) (a :& b :& c :& d :& xs) =
      fmap (\xs' -> a :& b :& c :& d :& xs') (go' p xs)
    go _ _ = error "rLens - unreachable"
    {-# INLINE go #-}

    go' :: Elem r rr -> Rec rr -> g (Rec rr)
    go' Here (x :& xs) = fmap (:& xs) (f x)
    go' (There p) (x :& xs) = fmap (x :&) (go p xs)
    go' _ _ = error "rLens - unreachable"
    {-# INLINABLE go' #-}

{-# INLINE rLens #-}

-- | view
rGet :: IElem (t ::: c) rs => (t ::: c) -> Rec rs -> c
rGet r = getConst . rLens r Const
{-# INLINE rGet #-}

-- | set
rSet :: IElem (t ::: a) rs => (t ::: a) -> a -> Rec rs -> Rec rs
rSet r x = runIdentity . rLens r (Identity . const x)
{-# INLINE rSet #-}

-- | over
rOver :: IElem (t ::: a) rs => (t ::: a) -> (a -> a) -> Rec rs -> Rec rs
rOver r f = runIdentity . rLens r (Identity . f)
{-# INLINE rOver #-}

class RecExpr a where
  mkRecExpr :: ExprBuilder -> a -> a


instance KnownSymbol t => RecExpr (Rec '[t ::: Expr a]) where
  mkRecExpr b _ = SField =: (Expr 0 nm)
    where nm = b <> D.fromList [escapeIdent $ T.pack (symbolVal (Proxy :: Proxy t))]


instance (KnownSymbol t, RecExpr (Rec b)) => RecExpr (Rec ((t ::: Expr a) ': b)) where
  mkRecExpr b _ = h <+> mkRecExpr b (undefined :: Rec b)
   where
     h =  SField =: (Expr 0 nm)
     nm = b <> D.fromList [escapeIdent $ T.pack (symbolVal (Proxy :: Proxy t))]

fieldName :: forall t a . KnownSymbol t => (t ::: a) -> T.Text
fieldName _ = T.pack $ symbolVal (Proxy :: Proxy t)

setR :: (KnownSymbol t, IElem (t ::: Expr a) s) => (t ::: Expr a) -> Expr a -> UpdExpr (Rec s)
setR fld (Expr _ a) = UpdExpr $ HashMap.singleton (fieldName fld) a

instance forall a r. (RecExpr (Rec a), r~(Rec a)) => FromItem (Table (Rec a)) (Rec a) where
  fromItem (Table nm) = From $ do
    let bs = T.encodeUtf8 nm
        nameBld = EscapeIdentifier bs
    alias <- B.fromByteString <$> grabName --Alias bs nameBld
    let r = mkRecExpr (builder (alias <> B.fromChar '.')) (undefined :: Rec a)
        q = pure nameBld <> raw " AS " <> (builder alias)
    return $ FromQuery q r


rupdate :: forall a b . RecExpr a => Table a -> (a -> Updating a b) -> Update b
rupdate (Table nm) f = Update $ do
  alias <- B.fromByteString <$> grabName
  let r = mkRecExpr (builder (alias <> B.fromChar '.')) (undefined :: a)
      q = pure nameBld <> raw " AS " <> (builder alias)
      upd = f r
  compileUpdating q upd
  where
      nameBld = EscapeIdentifier $ T.encodeUtf8 nm

rdelete :: forall a  b . RecExpr a => Table a -> (a -> Deleting a b) -> Update b
rdelete (Table nm) f = do
  alias <- B.fromByteString <$> grabName
  let r = mkRecExpr (builder (alias <> B.fromChar '.')) (undefined :: a)
      q = pure nameBld <> raw " AS " <> (builder alias)
      upd = f r
  compileDeleting q upd
  where
     nameBld = EscapeIdentifier $ T.encodeUtf8 nm


rinsert :: forall a b . RecExpr a => Table a -> Query (UpdExpr a) -> Update a
rinsert (Table nm) mq = do
  let r = mkRecExpr mempty (undefined :: a)
  (UpdExpr upd, q) <- compIt mq
  compileInserting (pure $ escapeIdent nm) $ q {queryAction = HashMap.toList upd}
  return r

(.>) :: IElem (t ::: c) rs => Rec rs -> (t ::: c) -> c
r .> f = rGet f r
infixl 8 .>
{-# INLINE (.>) #-}

newtype UpdaterM t a = UpdaterM (State (HashMap.HashMap T.Text ExprBuilder) a)

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

mkUpdate :: Updater (Rec a) -> UpdExpr (Rec a)
mkUpdate (UpdaterM u) = UpdExpr (execState u mempty)

setU :: (KnownSymbol t, IElem (t ::: Expr a) s) => (t ::: Expr a) -> Expr a -> Updater (Rec s)
setU fld (Expr _ a) = UpdaterM . modify $ HashMap.insert (fieldName fld) a

setVal :: (KnownSymbol t, IElem (t ::: Expr a) s, ToField a)
       => (t ::: Expr a) -> a -> Updater (Rec s)
setVal f v = setU f (term $ rawField v)
