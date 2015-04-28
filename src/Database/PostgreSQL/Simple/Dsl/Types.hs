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
import           Data.Vinyl.Core                  (Rec (..), rappend, (<+>))

import           GHC.TypeLits                     (KnownSymbol, Symbol, symbolVal)

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

recCons :: Functor f => f a -> Rec f rs -> Rec f (s :-> a ': rs)
recCons = (:&) . fmap Col
{-# INLINE recCons #-}

recUncons :: Functor f => Rec f (s :-> r ': rs) -> (f r, Rec f rs)
recUncons (x :& xs) = (fmap getCol x, xs)
{-# INLINE recUncons #-}

type PGRecord xs = Rec Identity xs

(=:) :: proxy (t :-> a) -> a -> PGRecord '[t :-> a]
_ =: a = Identity (Col a) :& RNil

(=::) :: proxy t a -> a -> PGRecord '[t :-> a]
_ =:: a = Identity (Col a) :& RNil

data SField a = SField

