{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}

module Database.PostgreSQL.Simple.Dsl.Record
  (
   Rec(..)
  , (=:)
  , (=::)
  , (<+>)
  , (.>)
  , (?>)
  , (.?>)
  , (=.)
  , (=.!)
  , Updater
  , UpdaterM
  , mkUpdate
  , setR
  , setField
  , setFieldVal
  , (:::)
  , IElem
  , ISubset
  , Proxy(..)
--  , PGRecord
  , RLens
  , (:->)(..)
  ) where

import           GHC.TypeLits                            hiding (Nat)

import           Control.Applicative                     (Const (..))
--import           Control.Monad.Identity
import           Control.Monad.Trans.State.Strict        (State, execState, modify)
import           Data.Coerce
import qualified Data.HashMap.Strict                     as HashMap
import           Data.Proxy
import qualified Data.Text                               as T

import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.Dsl.Types
import           Database.PostgreSQL.Simple.ToField

type RLens e rs = forall f . (IElem e rs, Functor f) => (e -> f e) -> Rec rs -> f (Rec rs)

view :: ((a -> Const a a) -> s -> Const a s) -> s -> a
view l = getConst . l Const

--(.>) :: IElem '(t, c) rs => FieldRec rs -> SField '(t, c) -> c
(.>) :: (IElem (t :-> v) rs) =>  Rec rs -> RLens (t :-> v) rs -> v
r .> f = getCol (view f r)

infixl 8 .>
{-# INLINE (.>) #-}

infixl 8 ?>
{-# INLINE (?>) #-}

(?>) :: (nc ~ Nulled c, IElem (t :-> Expr c) rs)
     => Nullable (Rec rs) -> RLens (t :-> Expr c) rs -> Expr nc
r ?> f = coerce $ getNullable r .> f

infixl 8 .?>
{-# INLINE (.?>) #-}
(.?>) :: (IElem (t :-> Expr c) rs)
     => Nullable (Rec rs) -> RLens (t :-> Expr c) rs -> Expr c
r .?> f = getNullable r .> f

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

type (:::) a b = Proxy (a :-> b)

mkUpdate :: Updater (Rec a) -> UpdExpr (Rec a)
mkUpdate (UpdaterM u) = UpdExpr (execState u mempty)

fieldName :: forall t a s. KnownSymbol t => RLens (t :-> a) s -> T.Text
fieldName _ = T.pack $ symbolVal (Proxy :: Proxy t)

setField :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
         RLens (t :-> Expr a) s -> Expr a -> Updater (Rec s)
setField fld (Expr _ a) = UpdaterM . modify $ HashMap.insert (fieldName fld) a

setFieldVal :: (KnownSymbol t,  ToField a, IElem (t :-> Expr a) s) =>
          RLens (t :-> Expr a) s -> a -> Updater (Rec s)
setFieldVal f v = setField f (term $ rawField v)

infixr 7 =., =.!
(=.) :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
         RLens (t :-> Expr a) s -> Expr a -> Updater (Rec s)
(=.) = setField

(=.!) :: (KnownSymbol t,  ToField a, IElem (t :-> Expr a) s) =>
          RLens (t :-> Expr a) s -> a -> Updater (Rec s)
(=.!) = setFieldVal

setR :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
     RLens (t :-> Expr a) s -> Expr a -> UpdExpr (Rec s)
setR fld (Expr _ a) = UpdExpr $ HashMap.singleton (fieldName fld) a

(=::) :: RLens (t :-> a) rs -> a -> PGRecord '[t :-> a]
_ =:: a = Col a :& RNil
