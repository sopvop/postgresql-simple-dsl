{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
module Database.PostgreSQL.Simple.Dsl
     (
     -- * Example of usage
     -- $use
       select
     , finish
     , formatQuery
     , fromTable
     , with
     , innerJoin
     , crossJoin
     , where_
     , project
     , orderBy
     , orderOn
     , ascendOn
     , descendOn
     , limitTo
     , offsetTo
     , val
     , whole
     , only
     , Whole
     , Only(..)
     , (:.)(..)
     , Select
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

-- $use
--
-- A bit of boilerplate is required
--
-- @
--
--      newtype UserId = UserId { getUserId :: Int}
--                  deriving (Show, ToField, FromField, Eq, Ord)
--
--      data User = User { userId :: UserId
--                 , userLogin :: String
--                 , userPassword :: ByteString
--                 } deriving (Show)
--
--      data Role = Role { roleUserId :: UserId, roleName :: ByteString }
--                  deriving (Show)
--
--      data instance Field User t a where
--        UserId'   :: Field User \"id\" UserId
--        UserLogin :: Field User \"login\" String
--        UserPass  :: Field User \"passwd\" ByteString
--
--      instance Table User where
--        tableName _ = \"users\"
--
--      instance Selectable User where
--        entityParser _ = User \<$> fromField UserId'
--                              \<*> fromField UserLogin
--                              \<*> fromField UserPass
--
--      instance Entity User where
--        type EntityId User = UserId
--        type EntityIdColumn User = \"id\"
--
--      data instance Field Role t a where
--        RoleUserId :: Field Role \"user_id\" UserId
--        RoleName   :: Field Role \"role\" ByteString
--
--      instance Table Role where
--        tableName _ = \"roles\"
--
--      instance Selectable Role where
--        entityParser _ = Role \<$> fromField RoleUserId
--                              \<*> fromField RoleName
--
--
--      getAllUsers :: Connection -> IO [Whole User]
--      getAllUsers c = select c $ fromTable
--
--      allUsers :: Select (Whole User)
--      allUsers = fromTable
--
--      allRoles :: Select (Whole Role)
--      allRoles = fromTable
-- @
--
--  This sql snippet maps to next code example
--
-- @
--      SELECT roles.role, role.user_id
--      FROM users
--      INNER JOIN roles ON users.id = roles.user_id
--      WHERE user.login = ?
-- @
--
-- @
--      userRoles :: String -> Select (ByteString, UserId)
--      userRoles login =
--        innerJoin (\\(usr :. rol) -> usr~>UserId' ==. rol~>RoleUserId)
--                  -- ^ joining on user.id == roles.user_id
--                  (allUsers & where_ (\u -> u~>UserLogin ==. val login))
--                  -- ^ restrict to user with needed login
--                  allRoles
--                  -- ^ select all roles
--        & project (\\(usr :. rol) -> (rol~>RoleName, rol~>RoleUserId))
--          -- select only roles.role and role.user_id fields
-- @
--
-- @
--     SELECT users.*, roles.*
--     FROM users, roles
--     WHERE users.id = role.user_id
-- @
--
-- @
--      userRoles2 = crossJoin fromTable fromTable
--                 & where_ (\\(u:.rol) -> u~>UserId' ==. rol~>RoleUserId)
-- @
--

-- | Start query with table - SELECT table.*
fromTable :: forall a . (Table a, Selectable a) => Select (Whole a)
fromTable = Select $ do
  nm <- grabName
  let fromWhat = plain (B.append tn " AS ") `D.cons` getBuilder nm
      expr = Expr nm :: Expr (Whole a)
  return $ mkSelector fromWhat expr
  where
    tn = tableName (undefined :: Proxy a)

-- | Adds non recursive WITH query
with :: (IsExpr with, IsExpr a) => Select with -> Select a -> Select (a :. with)
with (Select mw) (Select s) = Select $ do
  nm <- grabName
  w <- mw
  (compiled, subSelect) <- compileSelector w nm
  sq <- s
  let nameBld = getBuilder nm
      withAct = (nameBld `D.snoc` plain " AS (")
                `D.append` compiled `D.snoc` plain ") "
      newExpr = selectExpr sq :. subSelect
  return ( sq { selectWith = selectWith sq ++ [withAct]
              , selectExpr = newExpr
              , selectFrom = (selectFrom sq `D.snoc` plain ",") `D.append` nameBld } )

-- | Inner join on bool expression. Joins 'WHERE' clauses using AND
innerJoin :: (AsExpr a :. AsExpr b -> Expr Bool) -> Select a -> Select b
         -> Select (a :. b)
innerJoin f (Select left) (Select right) = Select $ do
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
  return $ (mkSelector fromClause (lexp :. rexp) ) { selectWhere = whereClause }

-- | Do a cross join, SELECT .. FROM a,b
crossJoin :: Select a -> Select b -> Select (a :. b)
crossJoin (Select left) (Select right) = Select $ do
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
  return $ (mkSelector fromClause (lexp :. rexp) ) { selectWhere = whereClause }

-- | Append expression to WHERE clause
where_ :: (AsExpr a -> Expr Bool) -> Select a -> Select a
where_ f (Select mq) = Select $ do
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
  let sort = D.concat . intersperse (D.singleton $ plain ",")
                      . map compileSorting . f
  return q { finishingOrder = Just sort }

-- | append to ORDER BY clase
orderOn :: (AsExpr a -> Sorting) -> Query a -> Query a
orderOn f (Query mq) = Query $ do
  q <- mq
  let compiled = compileSorting . f
      sort = case finishingOrder q of
                 Nothing -> compiled
                 Just _ -> fmap (`D.snoc` plain ",") compiled
  return q { finishingOrder = finishingOrder q <> Just sort }

ascendOn :: (Expr a) -> Sorting
ascendOn (Expr a) = Asc a

descendOn :: Expr t -> Sorting
descendOn (Expr a) = Desc a

-- | set LIMIT
limitTo :: Int -> Query a -> Query a
limitTo i (Query mq) = Query $ do
  q <- mq
  return $ q { finishingLimit = Just i }

-- | set OFFSET
offsetTo :: Int -> Query a -> Query a
offsetTo i (Query mq) = Query $ do
  q <- mq
  return $ q { finishingOffset = Just i }

-- | Select only subset of data
project :: (AsExpr a -> AsExpr b) -> Select a -> Select b
project f (Select mq) = Select $ do
    q <- mq
    let newExpr = f (selectExpr q)
    return $ q { selectExpr = newExpr }

finish :: IsExpr a => Select a -> Query a
finish (Select mselector) = Query $ do
  selector <- mselector
  let exprs = selectExpr selector
  (bld, _) <- compileSelector selector =<< grabName
  return $ mkFinishing bld exprs

-- | Execute query
select :: (IsExpr a, FromRow a) => Connection -> Query a -> IO [a]
select c q = query c "?" (Only $ finishQuery q)

formatQuery :: IsExpr a => Connection -> Query a -> IO ByteString
formatQuery c q = PG.formatQuery c "?" (Only $ finishQuery q)

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
