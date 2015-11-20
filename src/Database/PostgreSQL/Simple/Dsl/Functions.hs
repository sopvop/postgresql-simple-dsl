{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.PostgreSQL.Simple.Dsl.Functions
       ( count
       , countAll
       , avg_
       , sum_
       , max_
       , min_
       , coalesce
       , coalesce'
       , array_agg
       , array
       , array_append
       , array_prepend
       , array_cat
       , array_empty
       , least
       , greatest
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

avg_ :: Expr a -> ExprA (Nulled a)
avg_ (Expr _ a) = ExprA $ Expr 0 (byteString "avg(" <> a <> char8 ')')

sum_ :: Expr a -> ExprA (Nulled a)
sum_ (Expr _ a) = ExprA $ Expr 0 (byteString "sum(" <> a <> char8 ')')

max_ :: Expr a -> ExprA (Nulled a)
max_ (Expr _ a) = ExprA $ Expr 0 (byteString "max(" <> a <> char8 ')')

min_ :: Expr a -> ExprA (Nulled a)
min_ (Expr _ a) = ExprA $ Expr 0 (byteString "min(" <> a <> char8 ')')

array_agg :: (Expr a) -> ExprA (Maybe (PGArray a))
array_agg (Expr _ a) = ExprA . Expr 0 $ byteString "array_agg(" <> a <> byteString ")"


coalesce' :: IsExpr expr => expr (Maybe a) -> expr a -> expr a
coalesce' ea eb = fromExpr $ term $ byteString "coalesce("
          <> a <> char8 ',' <> b <> char8 ')'
 where
  (Expr _ !a) = toExpr ea
  (Expr _ !b) = toExpr eb

{-# SPECIALISE coalesce' :: Expr (Maybe a) -> Expr a -> Expr a #-}
{-# SPECIALISE coalesce' :: ExprA (Maybe a) -> ExprA a -> ExprA a #-}

coalesce :: IsExpr expr => expr a -> expr (Maybe a) -> expr a
coalesce = flip coalesce'

{-# SPECIALISE coalesce :: Expr a -> Expr (Maybe a) -> Expr a #-}
{-# SPECIALISE coalesce :: ExprA a -> ExprA (Maybe a) -> ExprA a #-}


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


least :: IsExpr expr => expr a -> expr a -> expr a
least ea eb = fromExpr $ Expr 0
    (byteString "least(" <> a <> char8 ',' <>  b <>  char8 ')')
  where
    (Expr _ a) = toExpr ea
    (Expr _ b) = toExpr eb

greatest :: IsExpr expr => expr a -> expr a -> expr a
greatest ea eb = fromExpr $ Expr 0
    (byteString "greatest(" <> a <> char8 ',' <>  b <>  char8 ')')
  where
    (Expr _ a) = toExpr ea
    (Expr _ b) = toExpr eb

{-# SPECIALIZE least :: Expr a -> Expr a -> Expr a #-}
{-# SPECIALIZE least :: ExprA a -> ExprA a -> ExprA a #-}


{-# SPECIALIZE greatest :: Expr a -> Expr a -> Expr a #-}
{-# SPECIALIZE greatest :: ExprA a -> ExprA a -> ExprA a #-}
