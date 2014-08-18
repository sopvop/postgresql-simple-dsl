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

import           Data.Int
import           Data.Monoid
import           Database.PostgreSQL.Simple.Types        (PGArray (..))

import           Database.PostgreSQL.Simple.Dsl.Internal

count :: Expr a -> ExprA Int64
count (Expr _ a) = ExprA . Expr 0 $ raw "count(" <> a <> raw ")"

countAll :: ExprA Int64
countAll = ExprA $ Expr 0 (raw "count(*)")

sum_ :: Num a => Expr a -> ExprA Int64
sum_ (Expr _ a) = ExprA $ Expr 0 (raw "sum(" <> a <> raw ")")

array_agg :: (Expr a) -> ExprA (Maybe (PGArray a))
array_agg (Expr _ a) = ExprA . Expr 0 $ raw "array_agg(" <> a <> raw ")"

coalesce' :: Expr (Maybe a) -> Expr a -> Expr a
coalesce' (Expr _ a) (Expr _ b) = term $ raw "coalesce(" <> a <> rawC ',' <> b <> rawC ')'

coalesce :: Expr a -> Expr (Maybe a) -> Expr a
coalesce = flip coalesce'


array :: Expr a -> Expr (PGArray a)
array (Expr _ r) = term $ raw "ARRAY["<> r <> raw "]"

array_append :: Expr (PGArray a) -> Expr a -> Expr (PGArray a)
array_append (Expr _ arr) (Expr _ v) = Expr 0 $ raw "array_append("
             <> arr <> raw "," <> v <> raw ")"

array_prepend :: Expr a -> Expr (PGArray a) -> Expr (PGArray a)
array_prepend (Expr _ v) (Expr _ arr) = Expr 0 $
  raw "array_prepend(" <> v <> raw "," <> arr <> raw ")"
array_cat :: Expr (PGArray a) -> Expr (PGArray a) -> Expr (PGArray a)
array_cat (Expr _ a) (Expr _ b) = Expr 0 $
  raw "array_cat(" <> a <> raw "," <> b <> raw ")"

array_empty :: (Expr (PGArray a))
array_empty = term $ raw "ARRAY[]"
