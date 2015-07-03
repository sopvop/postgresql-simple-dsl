{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}

module Database.PostgreSQL.Simple.Dsl.Record
  (
   Rec(..)
  , (=:)
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
  , (:->)(..)
  ) where

import           GHC.TypeLits                            hiding (Nat)

import           Control.Applicative                     (Const (..))
import           Control.Monad.Identity
import           Control.Monad.Trans.State.Strict        (State, execState, modify)
import           Data.Coerce
import qualified Data.HashMap.Strict                     as HashMap
import           Data.Proxy
import qualified Data.Text                               as T

import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.Dsl.Types
import           Database.PostgreSQL.Simple.ToField


--(.>) :: IElem '(t, c) rs => FieldRec rs -> SField '(t, c) -> c
(.>) :: IElem (t :-> c) rs => Rec rs -> proxy (t :-> c) -> c
r .> f = getCol $ rget f r

infixl 8 .>
{-# INLINE (.>) #-}

infixl 8 ?>
{-# INLINE (?>) #-}

(?>) :: (nc ~ Nulled c, IElem (t :-> Expr c) rs)
     => Nullable (Rec rs) -> proxy (t :-> Expr c) -> Expr nc
r ?> f = coerce . getCol $ rget f (getNullable r)

infixl 8 .?>
{-# INLINE (.?>) #-}
(.?>) :: (IElem (t :-> Expr c) rs)
     => Nullable (Rec rs) -> proxy (t :-> Expr c) -> Expr c
r .?> f = getCol $ rget f (getNullable r)

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

fieldName :: forall t a proxy. KnownSymbol t => proxy (t :-> a) -> T.Text
fieldName _ = T.pack $ symbolVal (Proxy :: Proxy t)

setField :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
         proxy (t :-> Expr a) -> Expr a -> Updater (Rec s)
setField fld (Expr _ a) = UpdaterM . modify $ HashMap.insert (fieldName fld) a

setFieldVal :: (KnownSymbol t,  ToField a, IElem (t :-> Expr a) s) =>
          proxy (t :-> Expr a) -> a -> Updater (Rec s)
setFieldVal f v = setField f (term $ rawField v)

infixr 7 =., =.!
(=.) :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
         proxy (t :-> Expr a) -> Expr a -> Updater (Rec s)
(=.) = setField

(=.!) :: (KnownSymbol t,  ToField a, IElem (t :-> Expr a) s) =>
          proxy (t :-> Expr a) -> a -> Updater (Rec s)
(=.!) = setFieldVal

setR :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
     proxy (t :-> Expr a) -> Expr a -> UpdExpr (Rec s)
setR fld (Expr _ a) = UpdExpr $ HashMap.singleton (fieldName fld) a

