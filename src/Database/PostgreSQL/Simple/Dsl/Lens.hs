{-# LANGUAGE RankNTypes #-}
-- | A minimal stub for record lens
module Database.PostgreSQL.Simple.Dsl.Lens
  where

import Control.Applicative        (Const (..))
import Control.Monad.Reader.Class (MonadReader, asks)
import Control.Monad.State.Class  (MonadState, modify, gets)
import Data.Functor.Identity      (Identity (..))

type Lens s t a b = forall f . Functor f => (a -> f b) -> s -> f t

type Lens' s a = forall f . Functor f => (a -> f a) -> s -> f s

view :: Lens s t a b -> s -> a
view l = getConst . l Const

set :: Lens s t a b -> b -> s -> t
set l v = runIdentity . l (Identity . const v)

over :: Lens s t a b -> (a -> b) -> s -> t
over l f = runIdentity . l (Identity . f)

infixl 8 ^.

(^.) :: s -> Lens s t a b -> a
v ^. l = view l v

infixr 4 .~, %~

(.~) :: Lens s t a b -> b -> s -> t
l .~ v = set l v

(%~) :: Lens s t a b -> (a -> b) -> s -> t
l %~ f = over l f


views :: MonadReader s m => Lens s t a b -> m a
views l = asks (view l)

use :: MonadState s m => Lens' s a -> m a
use l = gets (view l)

uses :: MonadState s m => Lens' s a -> (a -> b) -> m b
uses l f = gets (f . view l)

assign :: MonadState s m => Lens' s a -> a -> m ()
assign l v = modify (set l v)

modifying :: MonadState s m => Lens' s a -> (a -> a) -> m ()
modifying l f = modify (over l f)

infix 4 .=, %=

(.=) :: MonadState s m => Lens' s a -> a -> m ()
l .= v = assign l v

(%=) :: MonadState s m => Lens' s a -> (a -> a) -> m ()
l %= f = modifying l f
