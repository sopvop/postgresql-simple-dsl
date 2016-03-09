{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
module Database.PostgreSQL.Simple.Dsl.Internal.Types
  where

import Control.Applicative       ((<|>))
import Control.Monad             (replicateM_)
import Control.Monad.State.Class
import Control.Monad.Trans.State (State, StateT)

import Data.Monoid ((<>))

import Data.ByteString.Builder (Builder, char8, intDec)

import Database.PostgreSQL.Simple.FromField (FromField (..))
import Database.PostgreSQL.Simple.FromRow   (RowParser, field)
import Database.PostgreSQL.Simple.ToField   (Action (Plain), ToField (..))

import qualified Database.PostgreSQL.Simple.Types as PG


type RawExpr = Builder

-- | Adds phantom type to RawExpr
data Expr a = Expr {-# UNPACK #-} !Int RawExpr

instance ToField (Expr a) where
  toField (Expr _ a) = Plain a

rawExpr :: Expr a -> RawExpr
rawExpr (Expr _ b) = b


newtype NameSource = NameSource Int
  deriving (Show)

mkNameSource :: NameSource
mkNameSource = NameSource 0

class Monad m => HasNameSource m where
  grabNS :: m NameSource
  modifyNS :: (NameSource -> NameSource) -> m ()

instance Monad m => HasNameSource (StateT NameSource m) where
  grabNS = get
  modifyNS f = modify f
  {-# INLINE grabNS #-}
  {-# INLINE modifyNS #-}


grabName :: HasNameSource m => m Builder
grabName = do
  NameSource n <- grabNS
  modifyNS . const . NameSource $ succ n
  return $ char8 'q' `mappend` intDec n

{-# INLINE grabName #-}

newExpr :: HasNameSource m => m (Expr a)
newExpr = Expr 0 <$> grabName
{-# INLINE newExpr #-}


type family Nulled a where
  Nulled (Maybe a) = Maybe a
  Nulled b = Maybe b

newtype From a = From { runFrom :: State NameSource (RawExpr, a) }

-- | class for things which can be used in FROM clause
class FromItem f a | f -> a where
   fromItem :: f -> From a

instance FromItem (From a) a where
   fromItem = id

-- | Parser for entities with columns
data RecordParser a = RecordParser
  { recordColumns   :: [RawExpr]
  , recordRowParser :: RowParser a
  }

instance Functor RecordParser where
  fmap f (RecordParser cs p) = RecordParser cs (fmap f p)

instance Applicative RecordParser where
  pure =  RecordParser [] . pure
  RecordParser cf f <*> RecordParser ca a = RecordParser (cf <> ca) (f <*> a)

recField :: FromField a => Expr a -> RecordParser a
recField (Expr _ e) = RecordParser [e] field

takeField :: FromField a => Expr a -> RecordParser a
takeField = recField

nullableParser :: RecordParser a -> RecordParser (Maybe a)
nullableParser parser = RecordParser cols (pure Nothing <* replicateM_ (length cols) fnull
                                    <|> fmap Just rowP )
  where
    RecordParser cols rowP = parser
    fnull :: RowParser PG.Null
    fnull = field


