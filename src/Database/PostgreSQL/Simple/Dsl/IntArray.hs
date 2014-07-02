{-# LANGUAGE OverloadedStrings #-}
module Database.PostgreSQL.Simple.Dsl.IntArray
       where

import           Prelude                                 hiding (concat)

import           Data.Monoid

import           Database.PostgreSQL.Simple.Dsl.Internal

data IntArray
data QueryInt

intset :: Expr Int -> Expr IntArray
intset (Expr _ a) = Expr 0 $ raw "intset(" <> a <> rawC ')'

icount :: Expr IntArray -> Expr Int
icount (Expr _ a) = Expr 0 $ raw "icount(" <> a <> rawC ')'

sort_asc :: Expr IntArray -> Expr IntArray
sort_asc (Expr _ a) = Expr 0 $ raw "sort_asc(" <> a <> rawC ')'

sort_desc :: Expr IntArray -> Expr IntArray
sort_desc (Expr _ a) = Expr 0 $ raw "sort_desc(" <> a <> rawC ')'

-- | idx(int[], i)
idx :: Expr IntArray -> Expr Int -> Expr Int
idx (Expr _ a) (Expr _ i) = Expr 0 $ raw "idx(" <> a <> rawC ',' <> i <> rawC ')'

-- | subarray(int[], start, len)
subarray :: Expr IntArray -> Expr Int -> Expr Int -> Expr IntArray
subarray (Expr _ a) (Expr _ start) (Expr _ len)  =
      Expr 0 $ raw "subarray(" <> a <> rawC ',' <> start <> rawC ',' <> len <> rawC ')'

-- | subarray(int[], start)
subarray1 :: Expr IntArray -> Expr Int -> Expr IntArray
subarray1 (Expr _ a) (Expr _ start) =
      Expr 0 $ raw "subarray(" <> a <> rawC ',' <> start <> rawC ')'

-- | uniq(int[])
uniq :: Expr IntArray -> Expr IntArray
uniq (Expr _ a) = Expr 0 $ raw "uniq(" <> a <> rawC ')'

-- | operator &&
overlap :: Expr IntArray -> Expr IntArray -> Expr Bool
overlap a b = binOp 10 (plain "&&") a b

-- | operator @>
contains :: Expr IntArray -> Expr IntArray -> Expr Bool
contains a b = binOp 10 (plain "@>") a b

-- | operator <@
contained :: Expr IntArray -> Expr IntArray -> Expr Bool
contained a b = binOp 10 (plain "<@") a b

-- | int[] + int
append :: Expr IntArray -> Expr Int -> Expr IntArray
append a b = binOp 6 (plain "+") a b

-- | int[] + int[]
concat :: Expr IntArray -> Expr IntArray -> Expr IntArray
concat a b = binOp 6 (plain "+") a b

-- | int[] - int
remove :: Expr IntArray -> Expr Int -> Expr IntArray
remove a b = binOp 6 (plain "-") a b

-- | int[] - int[]
difference :: Expr IntArray -> Expr IntArray -> Expr IntArray
difference a b = binOp 6 (plain "-") a b

-- | int[] | int
add :: Expr IntArray -> Expr Int -> Expr IntArray
add a b = binOp 10 (plain "|") a b

-- | int[] | int[]
union :: Expr IntArray -> Expr IntArray -> Expr IntArray
union a b = binOp 10 (plain "|") a b

-- | int[] & int[]
intersection :: Expr IntArray -> Expr IntArray -> Expr IntArray
intersection a b = binOp 10 (plain "&") a b

-- | query_int @@ int[]
satisfies :: Expr QueryInt -> Expr IntArray -> Expr Bool
satisfies a b = binOp 10 (plain "@@") a b

-- | int[] ~~ query_int
satisfied :: Expr IntArray -> Expr QueryInt -> Expr Bool
satisfied a b = binOp 10 (plain "~~") a b
