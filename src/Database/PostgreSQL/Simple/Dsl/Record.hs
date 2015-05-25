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
  , SField(..)
  , PGRecord
  , (:->)(..)
  ) where

import           GHC.TypeLits

import           Control.Monad.Identity
import           Control.Monad.Trans.State.Strict        (State, execState, modify)
import           Data.Coerce
import qualified Data.HashMap.Strict                     as HashMap
import           Data.Proxy
import qualified Data.Text                               as T
import           Data.Vinyl.Core
import           Data.Vinyl.Lens

import           Data.Vinyl.TypeLevel                    (RIndex)

import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.Dsl.Types
import           Database.PostgreSQL.Simple.ToField


--(.>) :: IElem '(t, c) rs => FieldRec rs -> SField '(t, c) -> c
(.>) :: IElem (t :-> c) rs => PGRecord rs -> proxy (t :-> c) -> c
r .> f = getCol . runIdentity $ rget f r

infixl 8 .>
{-# INLINE (.>) #-}

infixl 8 ?>
{-# INLINE (?>) #-}
(?>) :: (nc ~ Nulled c, IElem (t :-> Expr c) rs)
     => Nullable (PGRecord rs) -> proxy (t :-> Expr c) -> Expr nc
r ?> f = coerce . getCol . runIdentity $ rget f (getNullable r)

infixl 8 .?>
{-# INLINE (.?>) #-}
(.?>) :: (IElem (t :-> Expr c) rs)
     => Nullable (PGRecord rs) -> proxy (t :-> Expr c) -> Expr c
r .?> f = getCol . runIdentity $ rget f (getNullable r)

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

type IElem r rs = RElem r rs (RIndex r rs)

type (:::) a b = SField (a :-> b)

mkUpdate :: Updater (PGRecord a) -> UpdExpr (PGRecord a)
mkUpdate (UpdaterM u) = UpdExpr (execState u mempty)

fieldName :: forall t a proxy. KnownSymbol t => proxy (t :-> a) -> T.Text
fieldName _ = T.pack $ symbolVal (Proxy :: Proxy t)

setField :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
         proxy (t :-> Expr a) -> Expr a -> Updater (PGRecord s)
setField fld (Expr _ a) = UpdaterM . modify $ HashMap.insert (fieldName fld) a

setFieldVal :: (KnownSymbol t,  ToField a, IElem (t :-> Expr a) s) =>
          proxy (t :-> Expr a) -> a -> Updater (PGRecord s)
setFieldVal f v = setField f (term $ rawField v)

infixr 7 =., =.!
(=.) :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
         proxy (t :-> Expr a) -> Expr a -> Updater (PGRecord s)
(=.) = setField

(=.!) :: (KnownSymbol t,  ToField a, IElem (t :-> Expr a) s) =>
          proxy (t :-> Expr a) -> a -> Updater (PGRecord s)
(=.!) = setFieldVal

setR :: (KnownSymbol t, IElem (t :-> Expr a) s) =>
     proxy (t :-> Expr a) -> Expr a -> UpdExpr (PGRecord s)
setR fld (Expr _ a) = UpdExpr $ HashMap.singleton (fieldName fld) a

