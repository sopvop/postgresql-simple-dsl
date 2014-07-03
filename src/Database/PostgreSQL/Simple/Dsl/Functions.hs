{-# LANGUAGE OverloadedStrings #-}
module Database.PostgreSQL.Simple.Dsl.Functions
       ( count
       , countAll
       , sum_
       , array_agg
       ) where

import           Data.Int
import           Data.Monoid
import           Database.PostgreSQL.Simple.Types        (PGArray (..))

import           Database.PostgreSQL.Simple.Dsl.Internal

count :: Expr a -> ExprA Int64
count (Expr _ a) = ExprAgg $ raw "count(" <> a <> raw ")"

countAll :: ExprA Int64
countAll = ExprAgg (raw "count(*)")

sum_ :: Num a => Expr a -> ExprA Int64
sum_ (Expr _ a) = ExprAgg (raw "sum(" <> a <> raw ")")

array_agg :: (Expr a) -> ExprA (PGArray a)
array_agg (Expr _ a) = ExprAgg $ raw "array_agg(" <> a <> raw ")"
