{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeOperators          #-}
module Database.PostgreSQL.Simple.Dsl.Internal.Record
  where

import Data.Char (isLower, isUpper, toLower)

import qualified Data.Text          as Text
import qualified Data.Text.Encoding as Text

import Database.PostgreSQL.Simple           ((:.) (..))
import Database.PostgreSQL.Simple.FromField (FromField)

import Database.PostgreSQL.Simple.Dsl.Escaping
import Database.PostgreSQL.Simple.Dsl.Internal.Types

import GHC.Generics

data ToColumnsConfig = ToColumnsConfig
  { toColumnsConfigName :: (Maybe (String -> String))
  }

defaultToColumnsConfig :: ToColumnsConfig
defaultToColumnsConfig = ToColumnsConfig Nothing

class GToColumnsCon f where
  gToColumnsCon :: (String -> String) -> RawExpr -> f a

instance Selector s => GToColumnsCon (S1 s (K1 i (Expr a))) where
  gToColumnsCon f prefix = M1 . K1 $ Expr 0 qname
     where
       qname = mappend prefix (escapeIdentifier $ Text.encodeUtf8 name)
       name = Text.pack . f $ selName (undefined :: t s (K1 i (Expr a)) p)

instance (GToColumnsCon a , GToColumnsCon b ) => GToColumnsCon (a :*: b) where
  gToColumnsCon f prefix = gToColumnsCon f prefix :*: gToColumnsCon f prefix


instance (GToColumnsCon cls, Datatype d) => GToColumnsCon (D1 d (C1 c cls)) where
  gToColumnsCon f = M1 . M1 . gToColumnsCon f


class GToColumns f where
  gToColumns :: ToColumnsConfig -> RawExpr -> f a

instance (GToColumnsCon cls, Datatype d) => GToColumns (D1 d (C1 c cls)) where
  gToColumns conf = M1 . M1 . gToColumnsCon f
    where
      f = case toColumnsConfigName conf of
             Nothing -> camelToSnake .  dropDT dtname
             Just f' -> f'
      dtname = datatypeName (undefined :: D1 d (C1 c cls) a)


dropDT :: String -> String -> String
dropDT r [] = r
dropDT [] _ = []
dropDT xs ('_':pre) = dropDT xs pre
dropDT r@(x:xs) (p:pre) = if p == toLower x
                          then dropDT xs pre
                          else r

camelToSnake :: String -> String
camelToSnake = map toLower . go2 . go1
    where go1 "" = ""
          go1 (x:u:l:xs) | isUpper u && isLower l = x : '_' : u : l : go1 xs
          go1 (x:xs) = x : go1 xs
          go2 "" = ""
          go2 (l:u:xs) | isLower l && isUpper u = l : '_' : u : go2 xs
          go2 (x:xs) = x : go2 xs

genericColumns :: forall a . (Generic a, GToColumns (Rep a))
               => ToColumnsConfig
               -> RawExpr
               -> a
genericColumns conf = to . gToColumns conf

class ToColumns a where
  toColumns :: RawExpr -> a
  default toColumns :: (Generic a, GToColumns (Rep a)) => RawExpr -> a
  toColumns = genericColumns defaultToColumnsConfig

class GIsRecord f where
  gGenNames :: HasNameSource m => m (f a)
  gAsValues :: f a -> [RawExpr]

instance GIsRecord (S1 s (K1 i (Expr a))) where
  gGenNames = M1 . K1 <$> newExpr
  gAsValues (M1 (K1 v)) = [rawExpr v]

instance (GIsRecord a , GIsRecord b ) => GIsRecord (a :*: b) where
  gGenNames = (:*:) <$> gGenNames <*> gGenNames
  gAsValues (a :*: b ) = gAsValues a ++ gAsValues b

instance (GIsRecord cls) => GIsRecord (D1 d (C1 c cls)) where
  gGenNames = M1 . M1 <$> gGenNames
  gAsValues (M1 (M1 v)) =  gAsValues v


genericGenNames :: forall a m . (Generic a, GIsRecord (Rep a), HasNameSource m) => m a
genericGenNames = to <$> gGenNames

genericAsValues :: (Generic a, GIsRecord (Rep a)) => a -> [RawExpr]
genericAsValues = gAsValues . from

class IsRecord a where
  genNames :: HasNameSource m => m a
  asValues :: a -> [RawExpr]
  default genNames :: (Generic a, GIsRecord (Rep a), HasNameSource m) => m a
  genNames = genericGenNames

  default asValues :: (Generic a, GIsRecord (Rep a)) => a -> [RawExpr]
  asValues = genericAsValues

instance IsRecord (Expr a, Expr b)
instance IsRecord (Expr a, Expr b, Expr c)
instance IsRecord (Expr a, Expr b, Expr c, Expr d)
instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e)
instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f)
instance IsRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)

instance IsRecord (Expr a) where
  asValues (Expr _ a) = [a]
  genNames = newExpr

instance (IsRecord a, IsRecord b) => IsRecord (a :. b) where
  genNames = (:.) <$> genNames
                  <*> genNames
  asValues ( a :. b) = asValues a ++ asValues b



class GFromRecord f g where
  gFromRecord :: f a -> RecordParser (g b)

instance FromField a => GFromRecord (S1 s (K1 i (Expr a))) (S1 s' (K1 i' a)) where
  gFromRecord (M1 (K1 e)) = M1 . K1 <$> takeField e

instance (GFromRecord a a', GFromRecord b b') => GFromRecord (a :*: b) (a' :*: b') where
  gFromRecord (a :*: b) = (:*:) <$> gFromRecord a <*> gFromRecord b

instance (GFromRecord a b) => GFromRecord (D1 d (C1 c a)) (D1 d' (C1 c' b)) where
  gFromRecord (M1 (M1 v)) = M1 . M1 <$> gFromRecord v


genericFromRecord :: (Generic a, Generic b, GFromRecord (Rep a) (Rep b)) => a -> RecordParser b
genericFromRecord a = to <$> gFromRecord (from a)

class FromRecord a b where
  fromRecord :: a -> RecordParser b
  default fromRecord :: (Generic a, Generic b, GFromRecord (Rep a) (Rep b)) => a -> RecordParser b
  fromRecord = genericFromRecord

newtype Nullable a = Nullable { getNullable :: a }
        deriving (Eq, Ord, Functor)

instance (FromField a) =>
         FromRecord (Expr a) a where
  fromRecord = recField

instance (FromField a, FromField b) =>
         FromRecord (Expr a, Expr b) (a,b) where
instance (FromField a, FromField b, FromField c) =>
         FromRecord (Expr a, Expr b, Expr c) (a,b,c) where
instance (FromField a, FromField b, FromField c, FromField d) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d) (a,b,c,d) where
instance (FromField a, FromField b, FromField c, FromField d, FromField e) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e) (a,b,c,d,e) where
instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f) (a,b,c,d,e, f) where
instance (FromField a, FromField b, FromField c, FromField d
         , FromField e, FromField f, FromField g) =>
         FromRecord (Expr a, Expr b, Expr c, Expr d, Expr e, Expr f, Expr g)
         (a,b,c,d,e,f,g) where

instance (FromRecord a a', FromRecord b b') => FromRecord (a :. b) (a':.b') where
  fromRecord (a:.b) = (:.) <$> fromRecord a <*> fromRecord b


instance (FromRecord a b) => FromRecord (Nullable a) (Maybe b) where
  fromRecord r = nullableParser parser
    where
      parser = fromRecord (getNullable r) :: RecordParser b
