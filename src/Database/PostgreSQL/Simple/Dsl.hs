{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
module Database.PostgreSQL.Simple.Dsl
     (
     -- * Example of usage
     -- $use
       query
     , executeQuery
     , formatQuery
     , fromTable
     , select
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
     , update
     , updateFrom
     , insert
     , insertFrom
     , delete
     , deleteFrom
     , queryUpdate
     , executeUpdate
     , formatUpdate
     , returning
     , (=.)
     , setField
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
import           Data.Int                                (Int64)
import           Data.List                               (intersperse)
import           Data.Monoid
import           Data.Proxy                              (Proxy)
import           GHC.TypeLits                            (SingI)

import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          FromRow, Only (..))
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
--      getAllUsers c = query c $ select fromTable
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
--   SELECT * FROM users WHERE id = $1
-- @
--
-- @
--   getUser :: Query (Whole User)
--   getUser x = select $ fromTable & where_ (\u->u~>UserId' ==. val x)
-- @
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
  let fromWhat = raw (B.append tn " AS ") <> getBuilder nm
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
      withAct = nameBld <> raw " AS (" <> compiled <> raw ") "
      newExpr = selectExpr sq :. subSelect
  return ( sq { selectWith = selectWith sq ++ [withAct]
              , selectExpr = newExpr
              , selectFrom = (selectFrom sq `fappend` raw ",") <> Just nameBld } )

-- | Inner join on bool expression. Joins 'WHERE' clauses using AND
innerJoin :: (AsExpr a :. AsExpr b -> Expr Bool) -> Select a -> Select b
         -> Select (a :. b)
innerJoin f (Select left) (Select right) = Select $ do
  leftq  <- left
  rightq <- right
  let lexp = selectExpr leftq
      rexp = selectExpr rightq
      cond = getBuilder . getRawExpr $ f (lexp :. rexp)
      fromClause =  (selectFrom leftq `fappend` raw " INNER JOIN ")
                    <> (selectFrom rightq `fappend` (raw " ON " <> cond))
      whereClause = case selectWhere leftq of
        Nothing -> selectWhere rightq
        Just ex -> case selectWhere leftq of
          Nothing -> Just ex
          Just ex' -> Just (mkAnd ex ex')
  return $ (mkSelector' fromClause (lexp :. rexp) ) { selectWhere = whereClause }

-- | Do a cross join, SELECT .. FROM a,b
crossJoin :: Select a -> Select b -> Select (a :. b)
crossJoin (Select left) (Select right) = Select $ do
  leftq  <- left
  rightq <- right
  let lexp = selectExpr leftq
      rexp = selectExpr rightq
      fromClause =  (selectFrom leftq `fappend` raw ",")
                    <> selectFrom rightq
      whereClause = case (selectWhere leftq, selectWhere rightq) of
        (Just l, Just r) -> Just $ mkAnd l r
        (Nothing, Nothing) -> Nothing
        (Just l, _) -> Just l
        (_, Just r) -> Just r
  return $ (mkSelector' fromClause (lexp :. rexp) ) { selectWhere = whereClause }

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
                 Just _ -> raw "," `fprepend` compiled
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

select :: IsExpr a => Select a -> Query a
select (Select mselector) = Query $ do
  selector <- mselector
  let exprs = selectExpr selector
  (bld, _) <- compileSelector selector =<< grabName
  return $ mkFinishing bld exprs

-- | Execute query
query :: (IsExpr a, FromRow a) => Connection -> Query a -> IO [a]
query c q = PG.query c "?" (Only $ finishQuery q)

executeQuery :: IsExpr r => Connection -> Query r -> IO Int64
executeQuery c q = PG.execute c "?" (Only $ finishQuery q)

formatQuery :: IsExpr a => Connection -> Query a -> IO ByteString
formatQuery c q = PG.formatQuery c "?" (Only $ finishQuery q)

-- | lift value to Expr
val :: (ToField a ) => a -> Expr a
val = Expr . RawTerm . D.singleton . toField

infix 4 ==., <., <=., >., >=.

(==.) :: Expr a -> Expr a -> Expr Bool
Expr a ==. Expr b = binOpE (plain "=") a b

(>.) :: Expr a -> Expr a -> Expr Bool
Expr a >. Expr b = binOpE (plain ">") a b

(>=.) :: Expr a -> Expr a -> Expr Bool
Expr a >=. Expr b = binOpE (plain ">=") a b

(<.) :: Expr a -> Expr a -> Expr Bool
Expr a <. Expr b = binOpE (plain "<") a b

(<=.) :: Expr a -> Expr a ->  Expr Bool
Expr a <=. Expr b = binOpE (plain "<=") a b

infixr 3 &&.
(&&.), (||.) :: Expr Bool -> Expr Bool -> Expr Bool
Expr a &&. Expr b = binOpE (plain " AND ") a b

infixr 2 ||.
Expr a ||. Expr b = binOpE (plain " OR ") a b

(~>) :: (SingI t) => Expr (Whole a) -> Field a t b -> Expr b
Expr lft ~> fld = Expr . RawExpr $ addParens lft <> raw (B.cons '.' f)
  where
    f = fieldColumn fld

-- | Select whole entity when projecting
whole :: Expr a -> Expr (Whole a)
whole (Expr a) = Expr a

-- | Wrap in Only
only :: Expr a -> Expr (Only a)
only (Expr a) = Expr a


update :: forall a. (Table a) => (AsExpr (Whole a) -> Expr Bool)
                     -> UpdExpr a -> Update (Whole a)
update f (UpdExpr upds) = Update $ do
  nm <- grabName
  let tableE = raw (B.append tn " AS ") <> getBuilder nm
      expr = Expr nm :: Expr (Whole a)
      Expr wher' = f expr
  return $ Updating (mkSelector mempty expr) { selectWhere = Just $ wher' }
         (DoUpdate upds) tableE
  where
    tn = tableName (undefined :: Proxy a)

updateFrom :: forall a from. (Table a) => Select from
                -> (AsExpr (from:. Whole a) -> Expr Bool)
                -> (AsExpr from -> UpdExpr a) -> Update (Whole a)
updateFrom (Select mfrom) f fu = Update $ do
  from' <- mfrom
  nm <- grabName
  let fromExpr = selectExpr from'
      tableE = raw (B.append tn " AS ") <> getBuilder nm
      expr = Expr nm :: Expr (Whole a)
      Expr wher' = f (fromExpr :. expr)
      UpdExpr upds = fu fromExpr
      where'' = case selectWhere from' of
        Nothing -> wher'
        Just w -> RawExpr $ addParens w <> raw " AND " <> addParens wher'
  return $ Updating (from' { selectExpr = expr,
                           selectWhere = Just $ where'' } ) (DoUpdate upds) tableE
  where
    tn = tableName (undefined :: Proxy a)

insert :: forall a . (Table a) => UpdExpr a -> Update (Whole a)
insert (UpdExpr upds) = Update $ do
  let tableE = D.singleton $ plain tn
      expr = Expr (RawTerm $ tableE) :: Expr (Whole a)
  return $ Updating (mkSelector mempty expr) (DoInsert upds) tableE
  where
    tn = tableName (undefined :: Proxy a)

insertFrom :: forall a from. (Table a) => Select from
           -> (AsExpr from -> UpdExpr a) -> Update (Whole a)
insertFrom (Select mfrom) fu = Update $ do
  from' <- mfrom
  let tableE = D.singleton $ plain tn
      expr = Expr (RawTerm $ tableE) :: Expr (Whole a)
      fromExpr = selectExpr from'
      UpdExpr upds = fu fromExpr
  return $ Updating (from' { selectExpr = expr } ) (DoInsert upds) tableE
  where
    tn = tableName (undefined :: Proxy a)


delete :: forall a. (Table a) => (AsExpr (Whole a) -> Expr Bool)
                    -> Update (Whole a)
delete f = Update $ do
  nm <- grabName
  let tableE = D.singleton $ plain tn
      expr = Expr nm :: Expr (Whole a)
      Expr wher' = f expr
  return $ Updating (mkSelector mempty expr) { selectWhere = Just $ wher' }
         DoDelete tableE
  where
    tn = tableName (undefined :: Proxy a)

deleteFrom :: forall a from. (Table a) => Select from
                -> (AsExpr (from:. Whole a) -> Expr Bool)
                -> Update (Whole a)
deleteFrom (Select mfrom) f = Update $ do
  from' <- mfrom
  nm <- grabName
  let fromExpr = selectExpr from'
      tableE = raw (B.append tn " AS ") <> getBuilder nm
      expr = Expr nm :: Expr (Whole a)
      Expr wher' = f (fromExpr :. expr)
      where'' = case selectWhere from' of
        Nothing -> wher'
        Just w -> RawExpr $ addParens w <> raw " AND " <> addParens wher'
  return $ Updating (from' { selectExpr = expr,
                           selectWhere = Just $ where'' } ) (DoDelete) tableE
  where
    tn = tableName (undefined :: Proxy a)

returning :: (AsExpr a -> AsExpr b) -> Update a -> Update b
returning f (Update mu) = Update $ do
  u <- mu
  let sel = updatingSelect u
      exprs = selectExpr sel
  return $ u { updatingSelect = sel { selectExpr = f exprs } }

queryUpdate :: (FromRow r, IsExpr r) => Connection -> Update r -> IO [r]
queryUpdate c q = PG.query c "?" (Only $ finishUpdate q)

executeUpdate :: IsExpr r => Connection -> Update r -> IO Int64
executeUpdate c q = PG.execute c "?" (Only $ finishUpdateNoRet q)


formatUpdate :: IsExpr a => Connection -> Update a -> IO ByteString
formatUpdate c q = PG.formatQuery c "?" (Only $ finishUpdate q)

infixr 7 =.
(=.) :: forall v t a. (SingI t, ToField a) => Field v t a -> Expr a -> UpdExpr v
f =. (Expr a) = UpdExpr [(fieldColumn f, a)]

setField :: forall v t a. (SingI t, ToField a) => Field v t a -> Expr a -> UpdExpr v
setField f a = f =. a
