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
       , array_contains
       , array_to_string
       , array_to_string2
       , least
       , greatest

       -- * range functions
       , rangeLower
       , rangeUpper
       , rangeIsEmpty
       , rangeLowerInf
       , rangeLowerInc
       , rangeUpperInf
       , rangeUpperInc

       , rangeElem
       , rangeContains
       , rangesOverlap
       , rangeAdjacentTo

       ) where

import           Data.ByteString.Builder (byteString, char8)
import           Data.Int
import           Data.Text (Text)
import           Data.Vector (Vector)
import           Database.PostgreSQL.Simple.Range (PGRange)

import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.Dsl.Types

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

array_agg :: (Expr a) -> ExprA (Maybe (Vector a))
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


array :: Expr a -> Expr (Vector a)
array (Expr _ r) = term $ byteString "ARRAY["<> r <> char8 ']'

array_append :: Expr (Vector a) -> Expr a -> Expr (Vector a)
array_append (Expr _ arr) (Expr _ v) = Expr 0 $ byteString "array_append("
             <> arr <> byteString "," <> v <> char8 ')'

array_prepend :: Expr a -> Expr (Vector a) -> Expr (Vector a)
array_prepend (Expr _ v) (Expr _ arr) = Expr 0 $
  byteString "array_prepend(" <> v <> byteString "," <> arr <> char8 ')'
array_cat :: Expr (Vector a) -> Expr (Vector a) -> Expr (Vector a)
array_cat (Expr _ a) (Expr _ b) = Expr 0 $
  byteString "array_cat(" <> a <> byteString "," <> b <> char8 ')'

array_empty :: (Expr (Vector a))
array_empty = term $ byteString "ARRAY[]"

-- | operator @>
array_contains :: Expr (Vector a) -> Expr (Vector a) -> Expr Bool
array_contains a b = binOp 10 (byteString "@>") a b

array_to_string :: Expr (Vector a) -> Expr Text -> Expr Text
array_to_string a b = call . arg b . arg a $ function "array_to_string"

array_to_string2 :: Expr (Vector a) -> Expr Text -> Expr Text -> Expr Text
array_to_string2 a b c = call . arg c . arg b . arg a $ function "array_to_string"

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


rangeLower :: Expr (PGRange a) -> Expr (Maybe a)
rangeLower r = call . arg r $ function "lower"

rangeUpper :: Expr (PGRange a) -> Expr (Maybe a)
rangeUpper r = call . arg r $ function "upper"

rangeIsEmpty :: Expr (PGRange a) -> Expr Bool
rangeIsEmpty r = call . arg r $ function "isempty"

rangeLowerInf :: Expr (PGRange a) -> Expr Bool
rangeLowerInf r = call . arg r $ function "lower_inf"

rangeLowerInc :: Expr (PGRange a) -> Expr Bool
rangeLowerInc r = call . arg r $ function "lower_inc"

rangeUpperInf :: Expr (PGRange a) -> Expr Bool
rangeUpperInf r = call . arg r $ function "upper_inf"

rangeUpperInc :: Expr (PGRange a) -> Expr Bool
rangeUpperInc r = call . arg r $ function "upper_inc"

rangeElem :: Expr (PGRange a) -> Expr a -> Expr Bool
rangeElem = binOp 10 "@>"

rangeContains :: Expr (PGRange a) -> Expr (PGRange a) -> Expr Bool
rangeContains = binOp 10 "@>"

rangesOverlap :: Expr (PGRange a) -> Expr (PGRange a) -> Expr Bool
rangesOverlap = binOp 10 "&&"

rangeAdjacentTo :: Expr (PGRange a) -> Expr (PGRange a) -> Expr Bool
rangeAdjacentTo = binOp 10 "-|-"
