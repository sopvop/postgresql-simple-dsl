{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE PolyKinds                  #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeOperators              #-}

module Database.PostgreSQL.Simple.Dsl.Types
       where

import           Control.Monad.Identity
import           Data.Proxy
import qualified Data.Text                        as T
import           Control.Applicative                  (Const (..))

import           GHC.TypeLits                     (KnownSymbol, symbolVal)

import           Database.PostgreSQL.Simple.Types (QualifiedIdentifier (..))

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

recCons ::  a -> Rec rs -> Rec (s :-> a ': rs)
recCons = (:&) . Col
{-# INLINE recCons #-}

recUncons :: Rec (s :-> r ': rs) -> (r, Rec rs)
recUncons (x :& xs) = (getCol x, xs)
{-# INLINE recUncons #-}

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


data Rec :: [*] -> * where
  RNil :: Rec '[]
  (:&) :: !r -> !(Rec rs) -> Rec (r ': rs)


rappend :: Rec as
        -> Rec bs
        -> Rec (as ++ bs)
rappend RNil ys = ys
rappend (x :& xs) ys = x :& (xs `rappend` ys)

(<+>) :: Rec as
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

