{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}

module Database.PostgreSQL.Simple.Dsl
     ( select
     , formatQuery
     , fromTable
     , with
     , innerJoin
     , crossJoin
     , where_
     , project
     , orderBy
     , orderOn
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
     , IsExpr
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

-- | Start query with table - SELECT table.*
fromTable :: forall a . (Table a, Selectable a) => Query (Whole a)
fromTable = Query $ do
  nm <- grabName
  let fromWhat = plain (B.append tn " AS ") `D.cons` getBuilder nm
      expr = Expr nm :: Expr (Whole a)
  return $ mkSelect fromWhat expr
  where
    tn = tableName (undefined :: Proxy a)

-- | Adds non recursive WITH query
with :: (IsExpr with, IsExpr a) => Query with -> Query a -> Query (a :. with)
with (Query w) (Query s) = Query $ do
  nm <- grabName
  wq <- w
  (subRenames, subSelect) <- makeSubRename nm $ selectExpr wq
  sq <- s
  let nameBld = getBuilder nm
      withAct = (nameBld `D.snoc` plain " AS (")
                `D.append` compileSelect wq { selectExpr = subRenames } `D.snoc` plain ") "
      newExpr = selectExpr sq :. subSelect
  return ( sq { selectWith = selectWith sq ++ [withAct]
              , selectExpr = newExpr
              , selectFrom = (selectFrom sq `D.snoc` plain ",") `D.append` nameBld } )

-- | Inner join on bool expression. Joins 'WHERE' clauses using AND
--   drops ORDER BY, LIMIT, OFFSET.
innerJoin :: (AsExpr a :. AsExpr b -> Expr Bool) -> Query a -> Query b
         -> Query (a :. b)
innerJoin f (Query left) (Query right) = Query $ do
  leftq  <- left
  rightq <- right
  let lexp = selectExpr leftq
      rexp = selectExpr rightq
      cond = getBuilder . getRawExpr $ f (lexp :. rexp)
      fromClause =  (selectFrom leftq `D.snoc` plain " INNER JOIN ")
                   `D.append` selectFrom rightq
                   `D.snoc` plain " ON " `D.append` cond
      whereClause = case selectWhere leftq of
        Nothing -> selectWhere rightq
        Just ex -> case selectWhere leftq of
          Nothing -> Just ex
          Just ex' -> Just (mkAnd ex ex')
  return $ (mkSelect fromClause (lexp :. rexp) ) { selectWhere = whereClause }

-- | Do a cross join, SELECT .. FROM a,b
crossJoin :: Query a -> Query b -> Query (a :. b)
crossJoin (Query left) (Query right) = Query $ do
  leftq  <- left
  rightq <- right
  let lexp = selectExpr leftq
      rexp = selectExpr rightq
      fromClause =  (selectFrom leftq `D.snoc` plain ",")
                   `D.append` selectFrom rightq
      whereClause = case selectWhere leftq of
        Nothing -> selectWhere rightq
        Just ex -> case selectWhere leftq of
          Nothing -> Just ex
          Just ex' -> Just (mkAnd ex ex')
  return $ (mkSelect fromClause (lexp :. rexp) ) { selectWhere = whereClause }

-- | Append expression to WHERE clause
where_ :: (AsExpr a -> Expr Bool) -> Query a -> Query a
where_ f (Query mq) = Query $ do
    q <- mq
    let Expr extra = f (selectExpr q)
        newSel = case selectWhere q of
           Nothing -> Just extra
           Just x -> Just $ mkAnd x extra
    return $ q { selectWhere = newSel }

-- | Replace ORDER BY clause
orderBy :: (AsExpr a -> [Sorting]) -> Query a -> Query a
orderBy f (Query mq) = Query $ do
  q <- mq
  let sort = Just . D.concat . intersperse (D.singleton $ plain ",")
                             . map compileSorting $ f (selectExpr q)
  return q { selectOrder = selectOrder q <> sort }

-- | append to ORDER BY clase
orderOn :: (AsExpr a -> Sorting) -> Query a -> Query a
orderOn f (Query mq) = Query $ do
  q <- mq
  let start = case selectOrder q of
                 Nothing -> mempty
                 Just order -> order `D.snoc` plain ","
      sort = Just $ start `D.append` compileSorting (f $ selectExpr q)
  return q { selectOrder = selectOrder q <> sort }

-- | set LIMIT
limitTo :: Int -> Query a -> Query a
limitTo i (Query mq) = Query $ do
  q <- mq
  return $ q { selectLimit = Just i }

-- | set OFFSET
offsetTo :: Int -> Query a -> Query a
offsetTo i (Query mq) = Query $ do
  q <- mq
  return $ q { selectOffset = Just i }

-- | Select only subset of data
project :: (AsExpr a -> AsExpr b) -> Query a -> Query b
project f (Query mq) = Query $ do
    q <- mq
    let newExpr = f (selectExpr q)
    return $ q { selectExpr = newExpr}

-- | Execute query
select :: (IsExpr a, FromRow a) => Connection -> Query a -> IO [a]
select c q = query c "?" (Only $ finishSelect q)

formatQuery :: IsExpr a => Connection -> Query a -> IO ByteString
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
