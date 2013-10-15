{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}

module Database.PostgreSQL.Simple.Dsl
     ( select
     , formatQuery
     , fromTable
     , with
     , innerJoin
     , where_
     , project
     , orderBy
     , limitTo
     , offsetTo
     , val
     , whole
     , only
     , Whole
     , Only(..)
     , (:.)(..)
     , Query
     , Expr
     , (==.), (<.), (<=.), (>.), (>=.), (||.), (&&.), (~>)
     , Field
     , Table (..)
     , Selectable (..)
     , fromField
     , Projection
     , EntityParser
     , entityRowParser
     ) where

import           Data.ByteString                         (ByteString)
import qualified Data.ByteString.Char8                   as B
import qualified Data.DList                              as D
import           Data.List                               (intersperse)
import           Data.Monoid
import           Data.Proxy                              (Proxy)

import           GHC.TypeLits                            (SingI)

import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          FromRow, Only (..),
                                                          query)
import qualified Database.PostgreSQL.Simple              as PG
import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.ToField


-- | Start query with table
fromTable :: forall a. (Table a, Selectable a) => Query (Expr (Whole a))
fromTable = Query $ do
  nm <- grabName
  let fromWhat = plain (B.append tn " AS ") `D.cons` getBuilder nm
      expr = Expr nm
  return $ (mkSelect fromWhat expr)
  where
    tn = tableName (undefined :: Proxy a)

-- | Add CTE query
with :: Projection with b => Query with -> Query a -> Query (a :. with)
with (Query w) (Query s) = Query $ do
  nm <- grabName
  wq <- w
  (subRenames, subSelect) <- makeSubSelects nm $ selectExpr wq
  sq <- s
  let nameBld = getBuilder nm
      withAct = (nameBld `D.snoc` plain " AS (")
                `D.append` compileSelect wq { selectExpr = subRenames } `D.snoc` plain ") "
      newExpr = selectExpr sq :. subSelect
  return ( sq { selectWith = selectWith sq ++ [withAct]
              , selectExpr = newExpr
              , selectFrom = (selectFrom sq `D.snoc` plain ",") `D.append` nameBld } )

-- | Inner join on bool expression
innerJoin :: (a :. b -> Expr Bool) -> Query a -> Query b
         -> Query (a :. b)
innerJoin f (Query left) (Query right) = Query $ do
  leftq  <- left
  rightq <- right
  let lexp = selectExpr leftq
      rexp = selectExpr rightq
      cond = getBuilder . getRawExpr $ f (lexp :. rexp)
      fromClause = ((selectFrom leftq `D.snoc` plain ",")
                   `D.snoc` plain " INNER JOIN ") `D.append` selectFrom rightq
                   `D.snoc` plain " ON " `D.append` cond
      whereClause = case selectWhere leftq of
        Nothing -> selectWhere rightq
        Just ex -> case selectWhere leftq of
          Nothing -> Just ex
          Just ex' -> Just (mkAnd ex ex')
  return $ (mkSelect fromClause (lexp :. rexp) ) { selectWhere = whereClause }


where_ :: (a -> Expr Bool) -> Query a -> Query a
where_ f (Query mq) = Query $ do
    q <- mq
    let Expr extra = f (selectExpr q)
        newSel = case selectWhere q of
           Nothing -> Just extra
           Just x -> Just $ mkAnd x extra
    return $ q { selectWhere = newSel }

orderBy :: (a -> [Sorting]) -> Query a -> Query a
orderBy f (Query mq) = Query $ do
  q <- mq
  let sort = Just . D.concat . intersperse (D.singleton $ plain ",")
                             . map compileSorting $ f (selectExpr q)
  return q { selectOrder = selectOrder q <> sort }

limitTo :: Int -> Query a -> Query a
limitTo i (Query mq) = Query $ do
  q <- mq
  return $ q { selectLimit = Just i }

offsetTo :: Int -> Query a -> Query a
offsetTo i (Query mq) = Query $ do
  q <- mq
  return $ q { selectOffset = Just i }

project :: (a -> b) -> Query a -> Query b
project f (Query mq) = Query $ do
    q <- mq
    let newExpr = f (selectExpr q)
    return $ q { selectExpr = newExpr}


select :: (Projection a b, FromRow b) => Connection -> Query a -> IO [b]
select c q = query c "?" (Only $ finishSelect q)

formatQuery :: Projection a b => Connection -> Query a -> IO ByteString
formatQuery c q = PG.formatQuery c "?" (Only $ finishSelect q)

-- | lift value to Expr
val :: (ToField a ) => a -> Expr a
val = Expr . RawTerm . D.singleton . toField

infix 4 ==., <., <=., >., >=.

(==.) :: Expr a -> Expr a -> Expr Bool
Expr a ==. Expr b = binOp (plain "=") a b

(>.) :: Expr a -> Expr a -> Expr Bool
Expr a >. Expr b = binOp (plain ">") a b

(>=.) :: Expr a -> Expr a -> Expr Bool
Expr a >=. Expr b = binOp (plain ">=") a b

(<.) :: Expr a -> Expr a -> Expr Bool
Expr a <. Expr b = binOp (plain "<") a b

(<=.) :: Expr a -> Expr a ->  Expr Bool
Expr a <=. Expr b = binOp (plain "<=") a b

infixr 3 &&.
(&&.), (||.) :: Expr Bool -> Expr Bool -> Expr Bool
Expr a &&. Expr b = binOp (plain " AND ") a b

infixr 2 ||.
Expr a ||. Expr b = binOp (plain " OR ") a b

(~>) :: (SingI t) => Expr (Whole a) -> Field a t b -> Expr b
Expr lft ~> fld = Expr . RawExpr $ addParens lft `D.snoc` (plain (B.cons '.' f))
  where
    f = fieldColumn fld

-- | Select whole entity when projecting
whole :: Expr a -> Expr (Whole a)
whole (Expr a) = Expr a

-- | Wrap in Only
only :: Expr a -> Expr (Only a)
only (Expr a) = Expr a
