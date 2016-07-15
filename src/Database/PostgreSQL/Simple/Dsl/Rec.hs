{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PolyKinds                  #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE TypeSynonymInstances       #-}
{-# LANGUAGE UndecidableInstances       #-}
module Database.PostgreSQL.Simple.Dsl.Rec
  (
   Rec(..)
  , (=:)
  , (=::)
  , (<+>)
--  , (.>)
  , (?>)
  , (.?>)
  , (:::)
  , IElem
  , ISubset
  , Proxy(..)
  , RLens
  , (:->)(..)
  , RElem (..)
  , RIndex (..)
  , RImage (..)
  , RSubset(..)
  , (++)
  , ELens
  , elens
  , lcol
  ) where


import           Control.Applicative     (Const (..))
import           Data.ByteString.Builder (char8)
import           Data.Coerce
import           Data.Monoid             ((<>))
import           Data.Proxy
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T

import Data.Functor.Identity (Identity (..))
import GHC.TypeLits          (KnownSymbol, symbolVal)

import Database.PostgreSQL.Simple.Types (QualifiedIdentifier (..))


import Database.PostgreSQL.Simple.Dsl.Escaping
import Database.PostgreSQL.Simple.Dsl.Internal
import Database.PostgreSQL.Simple.Dsl.Lens
import Database.PostgreSQL.Simple.Dsl.Types
import Database.PostgreSQL.Simple.FromField    (FromField (..))


class NamesColumn a where
  columnName :: Proxy a -> QualifiedIdentifier


instance KnownSymbol t => NamesColumn (t :-> a) where
  columnName _ = columnName (Proxy :: Proxy t)

instance KnownSymbol t => NamesColumn t where
  columnName _ = QualifiedIdentifier Nothing $ T.pack $ symbolVal (Proxy :: Proxy t)


newtype (:->) s a = Col { getCol :: a }
        deriving (Eq,Ord,Num,Monoid,Real,RealFloat,RealFrac,Fractional,Floating, Functor)

instance forall s a. (KnownSymbol s, Show a) => Show (s :-> a) where
    show (Col x) = symbolVal (Proxy::Proxy s)++" :-> "++show x

type PGRecord xs = Rec xs

(=:) :: proxy (t :-> a) -> a -> PGRecord '[t :-> a]
_ =: a = Col a :& RNil


data Nat = Z | S !Nat

type family RIndex (r :: k) (rs :: [k]) :: Nat where
  RIndex r (r ': rs) = 'Z
  RIndex r (s ': rs) = 'S (RIndex r rs)

type family RImage (rs :: [k]) (ss :: [k]) :: [Nat] where
  RImage '[] ss = '[]
  RImage (r ': rs) ss = RIndex r ss ': RImage rs ss

-- | Append for type-level lists.
type family (as :: [k]) ++ (bs :: [k]) :: [k] where
  '[] ++ bs = bs
  (a ': as) ++ bs = a ': (as ++ bs)

{-
data Rec :: [*] -> * where
  RNil :: Rec '[]
  (:&) :: !r -> !(Rec rs) -> Rec (r ': rs)
-}

data family Rec (l::[*])

data instance Rec '[] = RNil

data instance Rec (v ': rs) = !v :& !(Rec rs)

class RAppend as bs where
  rappend :: Rec as -> Rec bs -> Rec (as ++ bs)

instance RAppend '[] bs where
  rappend RNil b = b

instance RAppend as bs => RAppend (x ': as) bs where
  rappend (a :& as) bs = a :& rappend as bs

{-
rappend :: Rec as
        -> Rec bs
        -> Rec (as ++ bs)
rappend RNil ys = ys
rappend (x :& xs) ys = x :& (xs `rappend` ys)
-}
(<+>) :: RAppend as bs
      => Rec as
      -> Rec bs
      -> Rec (as ++ bs)
(<+>) = rappend

infixr 5  <+>

class i ~ RIndex r rs => RElem (r :: *) (rs :: [*]) (i :: Nat) where
  rlens :: Functor g
        => sing r
        -> (r -> g r)
        -> Rec rs
        -> g (Rec rs)

  rget :: sing r -> Rec rs -> r
  rget k = getConst . rlens k Const

  -- | For Vinyl users who are not using the @lens@ package, we also provide a
  -- setter. In general, it will be unambiguous what field is being written to,
  -- and so we do not take a proxy argument here.
  rput :: r
       -> Rec rs
       -> Rec rs
  rput y = runIdentity . rlens Proxy (\_ -> Identity y)

instance RElem r (r ': rs) 'Z where
  rlens _ f (x :& xs) = fmap (:& xs) (f x)
--  {-# INLINE rlens #-}
  {-# NOINLINE rlens #-}

instance (RIndex r (s ': rs) ~ 'S i, RElem r rs i) => RElem r (s ': rs) ('S i) where
  rlens p f (x :& xs) = fmap (x :&) (rlens p f xs)
--  {-# INLINE rlens #-}
  {-# NOINLINE rlens #-}
-- This is an internal convenience stolen from the @lens@ library.
lens :: Functor f
     => (t -> s)
     -> (t -> a -> b)
     -> (s -> f a)
     -> t
     -> f b
lens sa sbt afb s = fmap (sbt s) $ afb (sa s)
--{-# INLINE lens #-}


type IElem r rs = RElem r rs (RIndex r rs)

class is ~ RImage rs ss => RSubset (rs :: [*]) (ss :: [*]) is where

  -- | This is a lens into a slice of the larger record. Morally, we have:
  --
  -- > rsubset :: Lens' (Rec f ss) (Rec f rs)
  rsubset
    :: Functor g
    => (Rec rs -> g (Rec rs))
    -> Rec ss
    -> g (Rec ss)

  -- | The getter of the 'rsubset' lens is 'rcast', which takes a larger record
  -- to a smaller one by forgetting fields.
  rcast
    :: Rec ss
    -> Rec rs
  rcast = getConst . rsubset Const
--  {-# INLINE rcast #-}

  -- | The setter of the 'rsubset' lens is 'rreplace', which allows a slice of
  -- a record to be replaced with different values.
  rreplace
    :: Rec rs
    -> Rec ss
    -> Rec ss
  rreplace rs = runIdentity . rsubset (\_ -> Identity rs)
--  {-# INLINE rreplace #-}

instance RSubset '[] ss '[] where
  rsubset = lens (const RNil) const

instance (RElem r ss i , RSubset rs ss is) => RSubset (r ': rs) ss (i ': is) where
  rsubset = lens (\ss -> rget Proxy ss :& rcast ss) set
    where
      set :: Rec ss -> Rec (r ': rs) -> Rec ss
      set ss (r :& rs) = rput r $ rreplace rs ss

type ISubset rs ss = RSubset rs ss (RImage rs ss)

lcol :: Functor f => (a -> f a) -> (t :-> a) -> f (t :-> a)
lcol f (Col v) = (\c -> Col c) <$> f v

elens :: (Functor f, IElem (t :-> a) rs)
      => sing (t :-> a)
      -> (a -> f a)
      -> Rec rs
      -> f (Rec rs)
elens proxy = rlens proxy . lcol

type ELens t a rs = forall f . (IElem (t :-> a) rs, Functor f) => (a -> f a) -> Rec rs -> f (Rec rs)
type RLens e rs = forall f . (IElem e rs, Functor f) => (e -> f e) -> Rec rs -> f (Rec rs)
{-
view :: ((a -> Const a a) -> s -> Const a s) -> s -> a
view l = getConst . l Const
-}
--(.>) :: IElem '(t, c) rs => FieldRec rs -> SField '(t, c) -> c
(.>) :: (IElem (t :-> v) rs) =>  Rec rs -> Lens' (Rec rs) (t :-> v) -> v
r .> f = getCol (view f r)

infixl 8 .>
--{-# INLINE (.>) #-}

infixl 8 ?>
--{-# INLINE (?>) #-}

(?>) :: (nc ~ Nulled c, IElem (t :-> Expr c) rs)
     => Nullable (Rec rs) -> RLens (t :-> Expr c) rs -> Expr nc
r ?> f = coerce $ getNullable r .> f

infixl 8 .?>
--{-# INLINE (.?>) #-}
(.?>) :: (IElem (t :-> Expr c) rs)
     => Nullable (Rec rs) -> RLens (t :-> Expr c) rs -> Expr c
r .?> f = getNullable r .> f


type (:::) a b = Proxy (a :-> b)

(=::) :: RLens (t :-> a) rs -> a -> PGRecord '[t :-> a]
_ =:: a = Col a :& RNil


instance IsRecord (PGRecord ((t :-> Expr a) ': '[])) where
  asValues (a :& _) = [rawExpr $ getCol a]
  genNames = do
    n <- newExpr
    pure $ Proxy =: n

instance (IsRecord (PGRecord (b ': c))) =>
         IsRecord (PGRecord (t :-> Expr a ': b ': c)) where
  asValues (a :& as) = rawExpr (getCol a) : asValues as
  genNames = do
    n <- newExpr
    res <- genNames
    pure $ Proxy =: n <+> res


instance NulledRecord (PGRecord '[]) (PGRecord '[]) where
  nulled = id

instance forall a av nv bs nbs . (nv ~ Nulled av, NulledRecord (PGRecord bs) (PGRecord nbs))
  =>  NulledRecord (PGRecord (a :-> Expr av ': bs)) (PGRecord ( a :-> Expr nv ': nbs)) where
  nulled (a :& bs) = Proxy =: coerce a <+> nulled bs


instance FromField a => FromRecord (PGRecord '[t :-> Expr a]) (PGRecord '[ t :-> a]) where
  fromRecord (r :& _) = (Proxy =:) <$> recField (getCol r)

instance (FromRecord (PGRecord (b ': c)) (PGRecord (b' ': c')), FromField a) =>
   FromRecord (PGRecord (t :-> Expr a ': (b ': c)))
   (PGRecord ( t :-> a ': (b' ': c'))) where
  fromRecord (a :& as) = (\a' b' -> Proxy =: a' <+> b') <$> recField (getCol a)
                                                        <*> fromRecord as

instance KnownSymbol t => ToColumns (PGRecord '[t :-> Expr a ]) where
  toColumns b = Proxy =: (Expr 0 nm)
    where
     nm = b <> (escapeIdentifier . T.encodeUtf8 . T.pack $ symbolVal (Proxy :: Proxy t))

instance (KnownSymbol t, ToColumns (PGRecord (x ': xs)))
         => ToColumns (PGRecord (t :-> Expr a ': x ': xs )) where
  toColumns b = Proxy =: (Expr 0 nm)
                `rappend` toColumns b
    where
     nm = b <> (escapeIdentifier . T.encodeUtf8 . T.pack $ symbolVal (Proxy :: Proxy t))

