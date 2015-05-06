{-# LANGUAGE OverloadedStrings #-}
module Database.PostgreSQL.Simple.Dsl.IntArray
       where

import           Prelude                                 hiding (concat)

import           Data.ByteString.Builder                 (byteString, char8)
import           Data.Monoid

import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.Types        (PGArray (..))

data QueryInt

intset :: Expr Int -> Expr (PGArray Int)
intset (Expr _ a) = Expr 0 $ "intset(" <> a <> char8 ')'

icount :: Expr (PGArray Int) -> Expr Int
icount (Expr _ a) = Expr 0 $ "icount(" <> a <> char8 ')'

sort_asc :: Expr (PGArray Int) -> Expr (PGArray Int)
sort_asc (Expr _ a) = Expr 0 $ "sort_asc(" <> a <> char8 ')'

sort_desc :: Expr (PGArray Int) -> Expr (PGArray Int)
sort_desc (Expr _ a) = Expr 0 $ "sort_desc(" <> a <> char8 ')'

-- | idx(int[], i)
idx :: Expr (PGArray Int) -> Expr Int -> Expr Int
idx (Expr _ a) (Expr _ i) = Expr 0 $ "idx(" <> a <> char8 ',' <> i <> char8 ')'

-- | subarray(int[], start, len)
subarray :: Expr (PGArray Int) -> Expr Int -> Expr Int -> Expr (PGArray Int)
subarray (Expr _ a) (Expr _ start) (Expr _ len)  =
      Expr 0 $ "subarray(" <> a <> char8 ',' <> start <> char8 ',' <> len <> char8 ')'

-- | subarray(int[], start)
subarray1 :: Expr (PGArray Int) -> Expr Int -> Expr (PGArray Int)
subarray1 (Expr _ a) (Expr _ start) =
      Expr 0 $ "subarray(" <> a <> char8 ',' <> start <> char8 ')'

-- | uniq(int[])
uniq :: Expr (PGArray Int) -> Expr (PGArray Int)
uniq (Expr _ a) = Expr 0 $ "uniq(" <> a <> char8 ')'

-- | operator &&
overlap :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr Bool
overlap a b = binOp 10 (byteString"&&") a b

-- | operator @>
contains :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr Bool
contains a b = binOp 10 (byteString "@>") a b

-- | operator <@
contained :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr Bool
contained a b = binOp 10 (byteString "<@") a b

-- | int[] + int
append :: Expr (PGArray Int) -> Expr Int -> Expr (PGArray Int)
append a b = binOp 6 (char8 '+') a b

-- | int[] + int[]
concat :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr (PGArray Int)
concat a b = binOp 6 (char8 '+') a b

-- | int[] - int
remove :: Expr (PGArray Int) -> Expr Int -> Expr (PGArray Int)
remove a b = binOp 6 (char8 '-') a b

-- | int[] - int[]
difference :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr (PGArray Int)
difference a b = binOp 6 (char8 '-') a b

-- | int[] | int
add :: Expr (PGArray Int) -> Expr Int -> Expr (PGArray Int)
add a b = binOp 10 (char8 '|') a b

-- | int[] | int[]
union :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr (PGArray Int)
union a b = binOp 10 (char8 '|') a b

-- | int[] & int[]
intersection :: Expr (PGArray Int) -> Expr (PGArray Int) -> Expr (PGArray Int)
intersection a b = binOp 10 (char8 '&') a b

-- | query_int @@ int[]
satisfies :: Expr QueryInt -> Expr (PGArray Int) -> Expr Bool
satisfies a b = binOp 10 (byteString "@@") a b

-- | int[] ~~ query_int
satisfied :: Expr (PGArray Int) -> Expr QueryInt -> Expr Bool
satisfied a b = binOp 10 (byteString "~~") a b
