{-# LANGUAGE OverloadedStrings #-}
module Database.PostgreSQL.Simple.Dsl.Functions
       ( count
       , countAll
       , sum_
       , coalesce
       , coalesce'
       , array_agg
       , array
       , array_append
       , array_prepend
       , array_cat
       , array_empty
       ) where

import           Data.ByteString.Builder                 (byteString, char8)
import           Data.Int
import           Data.Monoid
import           Database.PostgreSQL.Simple.Types        (PGArray (..))

import           Database.PostgreSQL.Simple.Dsl.Internal

count :: Expr a -> ExprA Int64
count (Expr _ a) = ExprA . Expr 0 $ byteString "count(" <> a <> char8 ')'

countAll :: ExprA Int64
countAll = ExprA $ Expr 0 (byteString "count(*)")

sum_ :: Num a => Expr a -> ExprA Int64
sum_ (Expr _ a) = ExprA $ Expr 0 (byteString "sum(" <> a <> char8 ')')

array_agg :: (Expr a) -> ExprA (Maybe (PGArray a))
array_agg (Expr _ a) = ExprA . Expr 0 $ byteString "array_agg(" <> a <> byteString ")"

coalesce' :: Expr (Maybe a) -> Expr a -> Expr a
coalesce' (Expr _ a) (Expr _ b) = term $ byteString "coalesce("
          <> a <> char8 ',' <> b <> char8 ')'

coalesce :: Expr a -> Expr (Maybe a) -> Expr a
coalesce = flip coalesce'


array :: Expr a -> Expr (PGArray a)
array (Expr _ r) = term $ byteString "ARRAY["<> r <> char8 ']'

array_append :: Expr (PGArray a) -> Expr a -> Expr (PGArray a)
array_append (Expr _ arr) (Expr _ v) = Expr 0 $ byteString "array_append("
             <> arr <> byteString "," <> v <> char8 ')'

array_prepend :: Expr a -> Expr (PGArray a) -> Expr (PGArray a)
array_prepend (Expr _ v) (Expr _ arr) = Expr 0 $
  byteString "array_prepend(" <> v <> byteString "," <> arr <> char8 ')'
array_cat :: Expr (PGArray a) -> Expr (PGArray a) -> Expr (PGArray a)
array_cat (Expr _ a) (Expr _ b) = Expr 0 $
  byteString "array_cat(" <> a <> byteString "," <> b <> char8 ')'

array_empty :: (Expr (PGArray a))
array_empty = term $ byteString "ARRAY[]"
