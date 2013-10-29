{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
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
     -- , select
     , with
     , from
     , table
     , innerJoin
     , crossJoin
     , subSelect
     , where_
     , orderBy
     , ascendOn
     , descendOn
     , limitTo
     , offsetTo
     {-
     , Update
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
     , (=.!)
     , setField -}
     , val
     , whole
     , only
     , Whole(..)
     , Only(..)
     , (:.)(..)
     , From
     , Query
     , Expr, Rel
     , (==.), (<.), (<=.), (>.), (>=.), (||.), (&&.), (~>), ( ~.)
     , true, false, isNull, isInList
     , Field
     , Table (..)
     , Record (..)
     , takeField
     , IsExpr
     , RecordParser
     , recordRowParser
{-     , UpdExpr
     , Function
     , arg
     , function
     , call
     -}
     ) where
import           Control.Applicative
import           Control.Monad.State.Class
import           Control.Monad.Trans
import           Control.Monad.Trans.State               (runState)
import           Control.Monad.Trans.Writer

import           Data.ByteString                         (ByteString)
import qualified Data.ByteString.Char8                   as B
import qualified Data.DList                              as D
import           Data.Int                                (Int64)
import           Data.List                               (intersperse)
import           Data.Monoid
import           Data.Proxy                              (Proxy (..))
import           Data.Text                               (Text)
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

class (Monad m) => IsQuery m where
  with :: (IsExpr a, IsExpr b) => Query a -> (a -> m b) -> m b
  from :: From a -> m a
  where_ :: Expr Bool -> m ()

table :: (Record a) => ByteString -> From (Rel a)
table bs = From $ do
  nm <- grabName
  let as = raw "\"" <> nm <> raw "\""
      from' = raw "\"" <> raw bs <> raw "\" AS " <> as
  return $ FromTable from' (Rel as)

-- | Start query with table - SELECT table.*
fromTable :: forall a.(Table a, Record a) => Query (Rel a)
fromTable = from . table $ tableName (Proxy :: Proxy a)


instance IsQuery Query where
--   with :: (IsExpr a, IsExpr b) => Query a -> (a -> Query b) -> Query b
   with q f = do
      r <- Query $ do
         (r, w) <- prepareWith q
         lift $ tell w
         return r
      f r
   where_ (Expr r) = Query $ do
     lift $ tell mempty {queryStateWhere = Just r}
   from f = Query $ do
     (r, w) <- prepareFrom f
     lift $ tell w
     return r

prepareWith :: (MonadState NameSource m, IsExpr t) => Query t -> m (t, QueryState)
prepareWith q = do
   (r, q') <- compIt q
   nm <- grabName
   (proj, r') <- compileProjection r
   let bld = nm  <> raw " AS (" <> finishIt proj q' <> raw ")"
   return (r', mempty { queryStateWith = Just bld, queryStateFrom = Just nm  })

prepareFrom :: MonadState NameSource m => From t -> m (t, QueryState)
prepareFrom (From mfrm) = do
   ns <- get
   let (frm, ns') = runState mfrm ns
       (cmp, expr) = compileFrom frm
   put ns'
   return (expr, mempty { queryStateFrom = Just cmp })

innerJoin :: forall a b .(IsExpr a, IsExpr b, Table b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
innerJoin (From mfa) (From mfb) f = From $ do
    fa <- mfa
    fb <- mfb
    let Expr onexpr = f (fromExpr fa) (fromExpr fb)
    return $ FromInnerJoin fa fb onexpr

crossJoin :: forall a b .(IsExpr a, IsExpr b, Table b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
crossJoin (From fa) (From fb) f = From $ FromCrossJoin <$> fa <*> fb


subSelect :: (IsExpr a) => Query a -> Query a
subSelect q = Query $ do
  (r, q') <- compIt q
  nm <- grabName
  (proj, r') <- compileProjection r
  let bld = raw "(" <> finishIt proj q' <> raw ") AS " <> nm
  lift $ tell (mempty { queryStateFrom = Just bld})
  return r'


-- | append to ORDER BY clase
orderBy :: Sorting -> Query ()
orderBy f = Query $ do
  let compiled = compileSorting f
  lift $ tell mempty { queryStateOrder = Just $ compileSorting f }

ascendOn :: (Expr a) -> Sorting
ascendOn (Expr a) = Sorting [a <> raw " DESC"]

descendOn :: Expr t -> Sorting
descendOn (Expr a) = Sorting [a <> raw " ASC"]

-- | set LIMIT
limitTo :: Int -> Query ()
limitTo i = Query . lift $ tell mempty { queryStateLimit = Just i }

-- | set OFFSET
offsetTo :: Int -> Query ()
offsetTo i = Query . lift $ tell mempty { queryStateOffset = Just i }

-- | Execute query
query :: (IsExpr a, FromRow (FromExpr a)) => Connection -> Query a -> IO [FromExpr a]
query c q = PG.query c "?" (Only $ finishQuery q)

executeQuery :: IsExpr r => Connection -> Query r -> IO Int64
executeQuery c q = PG.execute c "?" (Only $ finishQuery q)

formatQuery :: IsExpr a => Connection -> Query a -> IO ByteString
formatQuery c q = PG.formatQuery c "?" (Only $ finishQuery q)

-- | lift value to Expr
val :: (ToField a ) => a -> Expr a
val = Expr . D.singleton . toField

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

(~>) :: (SingI t) => Rel a -> Field a t b -> Expr b
(Rel r) ~> fld = Expr $ r <> raw "." <> raw "\"" <> raw (fieldColumn fld) <> raw "\""
(RelAliased r) ~> fld = Expr $ raw "\"" <> r <> raw "__" <> raw (fieldColumn fld) <> raw "\""

(~.) :: Expr Text -> Expr Text -> Expr Bool
(~.) (Expr a) (Expr b) = binOpE (plain " ~ ") a b

isInList :: ToField a => (Expr a) -> [a] -> Expr Bool
isInList (Expr a) l = binOpE (plain " IN ") a (lst)
  where
    lst = D.fromList . intersperse (plain ",") $ map toField l

-- | Select whole entity when projecting
whole :: Expr a -> Expr (Whole a)
whole (Expr a) = Expr a

-- | Wrap in Only
only :: Expr a -> Expr (Only a)
only (Expr a) = Expr a

true :: Expr Bool
true = Expr $ raw "true"

false :: Expr Bool
false = Expr $ raw "true"

isNull :: Expr a -> Expr Bool
isNull (Expr r) = Expr $ addParens r <> raw " IS NULL"

instance IsQuery (Inserting r) where
   with q f = do
      r <- Inserting $ do
         (r, w) <- prepareWith q
         lift $ tell (mempty {insertWriterFrom = w})
         return r
      f r
   where_ (Expr r) = Inserting $ do
     lift $ tell mempty {insertWriterFrom = mempty {queryStateWhere = Just r}}
   from f = Inserting $ do
     (r, w) <- prepareFrom f
     lift $ tell (mempty { insertWriterFrom = w })
     return r

class IsUpdate m where
  setFields :: UpdExpr t -> m t ()

instance IsUpdate Inserting where
  setFields (UpdExpr x) = Inserting . lift $ tell (mempty { insertWriterSets = x })

instance IsUpdate Updating where
  setFields (UpdExpr x) = Updating . lift $ tell (mempty { insertWriterSets = x })


infixr 7 =., =.!
(=.) :: forall v t a. (SingI t) => Field v t a -> Expr a -> UpdExpr v
f =. (Expr a) = UpdExpr [(fieldColumn f, a)]

(=.!) :: (SingI t, ToField a) => Field v t a -> a -> UpdExpr v
(=.!) f e = f =. val e

set :: forall v t a. (SingI t, ToField a) => Field v t a -> Expr a -> UpdExpr v
set f a = f =. a

setField :: (SingI t, IsUpdate m) => Field v t a -> Expr a -> m v ()
setField f a = setFields (f =. a)

updateTable :: forall a b. Table a => (Rel a -> Updating a b) -> Update b
updateTable f = Update $ do
    nm <- grabName
    let r = Rel nm
    compileUpdating nm $ f r
  where
    table = tableName (Proxy :: Proxy a)
{-
update :: forall a. (Table a) => (AsExpr (Whole a) -> Expr Bool)
                     -> UpdExpr a -> Update (Whole a)
update f (UpdExpr upds) = Update $ do
  nm <- grabName
  let tableE = raw (B.append tn " AS ") <> getBuilder nm
      expr = Expr nm :: Expr (Whole a)
      Expr wher' = f expr
  return $ Updating (mkSelector' Nothing expr) { selectWhere = Just $ wher' }
         (DoUpdate upds) tableE
  where
    tn = tableName (Proxy :: Proxy a)

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
    tn = tableName (Proxy :: Proxy a)

insert :: forall a . (Table a) => UpdExpr a -> Update (Whole a)
insert (UpdExpr upds) = Update $ do
  let tableE = D.singleton $ plain tn
      expr = Expr (RawTerm $ tableE) :: Expr (Whole a)
  return $ Updating (mkSelector' Nothing expr) (DoInsert upds) tableE
  where
    tn = tableName (Proxy :: Proxy a)

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
    tn = tableName (Proxy :: Proxy a)


delete :: forall a. (Table a) => (AsExpr (Whole a) -> Expr Bool)
                    -> Update (Whole a)
delete f = Update $ do
  nm <- grabName
  let tableE = raw (B.append tn " AS ") <> getBuilder nm
      expr = Expr nm :: Expr (Whole a)
      Expr wher' = f expr
  return $ Updating (mkSelector' Nothing expr) { selectWhere = Just $ wher' }
         DoDelete tableE
  where
    tn = tableName (Proxy :: Proxy a)

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
    tn = tableName (Proxy :: Proxy a)

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

infixr 7 =., =.!
(=.) :: forall v t a. (SingI t) => Field v t a -> Expr a -> UpdExpr v
f =. (Expr a) = UpdExpr [(fieldColumn f, a)]

(=.!) :: (SingI t, ToField a) => Field v t a -> a -> UpdExpr v
(=.!) f e = f =. val e

setField :: forall v t a. (SingI t, ToField a) => Field v t a -> Expr a -> UpdExpr v
setField f a = f =. a
-}

