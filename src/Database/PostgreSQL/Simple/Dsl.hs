{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
module Database.PostgreSQL.Simple.Dsl
     (
     -- * Example of usage
     -- $use
     -- * Defining Records
       Record (..)
     , takeField
     , RecordParser
     , recordRowParser

     -- * Exequting queries
     , Query
     , query
     , execute
     , formatQuery
     -- * From
     , Table (..)
     , From
     , from
     , table
     , fromTable
     , innerJoin
     , crossJoin
     , subSelect
     -- * With
     , with
     , withUpdate
     -- * Query
     , where_
     , orderBy
     , ascendOn
     , descendOn
     , limitTo
     , offsetTo
     -- * Updates
     , Update
     , UpdExpr
     , Updating
     , update
     , updateTable
     , Inserting
     , insert
     , insertIntoTable
     , Deleting
     , delete
     , deleteFromTable
     , set
     , (=.)
     , (=.!)
     , setField
     , setFields
     -- * Executing updates
     , queryUpdate
     , executeUpdate
     , formatUpdate
     -- * Expressions
     , Rel, (~>)
     , Expr
     , val
     , (==.), (<.), (<=.), (>.), (>=.), (||.), (&&.), ( ~.)
     , true, false, isNull, isInList
     , whole
     , only
     -- * Helpers
     , Whole(..)
     , Only(..)
     , (:.)(..)
     -- * Helpers
     , IsExpr
     , FromExpr
     -- * Functions
     , Function
     , function
     , arg
     , call
     ) where
import           Control.Applicative
import           Control.Monad.State.Class
import           Control.Monad.Trans
import           Control.Monad.Trans.State               (runState)
import           Control.Monad.Trans.Writer

import           Data.ByteString                         (ByteString)
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
--      instance Record User where
--        data Field User t a where
--          UserKey   :: Field User \"id\" UserId
--          UserLogin :: Field User \"login\" String
--          UserPass  :: Field User \"passwd\" ByteString
--          UserIsDeleted :: Field User \"is_deleted\" Bool
--        recordParser _ = User \<$> fromField UserId'
--                              \<*> fromField UserLogin
--                              \<*> fromField UserPass
--                              \<* takeField UserIsDelete
-- @
--
-- Note that UserIsDeleted fieds is not used in parsers, but is still required
-- to mention
--
-- @
--      instance Table User where
--        tableName _ = \"users\"
--
--
--      instance Entity User where
--        type EntityId User = UserId
--        idField w = w~>UserKey
--
--      instance Record Role where
--        data Field Role t a where
--          RoleUserId :: Field Role \"user_id\" UserId
--          RoleName   :: Field Role \"role\" ByteString
--        recordParser \_ = Role \<$> fromField RoleUserId
--                              \<*> fromField RoleName
--
--
--      roles = table \"roles\" :: From (Rel Role)
--
--      allUsers2 = fromTable :: Query (Rel User)
--      allRoles = from roles
-- @
--
--  This sql snippet maps to next code example
--
-- @
--   SELECT * FROM users WHERE id = $1
-- @
--
--
-- @
--    getUser :: UserId -> Query (Rel User)
--    getUser uid = do
--      u <- fromTable
--      where_ $  u~>UserKey ==. val uid
--      return u
-- @
--
--
-- @
--      SELECT roles.role, role.user_id
--      FROM users, roles
--      WHERE users.id = roles.user_id AND user.login = $1
-- @
--
--
-- @
--      userRoles :: String -> Query (Rel User :. Rel Role)
--      userRoles login = do
--          u <- fromTable
--          r <- from roles
--          where $ u~>UserKey ==. r~>RoleUserId &&. u~>UserLogin ==. val login
--          return (u:.r)
-- @
--
-- With explicit joins. Not recommended for postgres, unless you join more that 6 tables
-- and optimizer is too slow.
--
-- @
--      SELECT roles.role, role.user_id
--      FROM users INNER JOIN roles ON users.id = roles.user_id
--      WHERE user.login = $1
-- @
--
--
-- @
--     userRoles2 login = do
--       r\@(u:.r) <- from \$ innerJoin roles recordTable
--                        \$ \\r u -> r~>RoleUserId ==. u~>UserKey
--       where $ u~>UserLogin ==. val login
--       return r
-- @
--
--

class (Monad m) => IsQuery m where
  with :: (IsExpr a, IsExpr b) => Query a -> (a -> m b) -> m b
  withUpdate  :: (IsExpr a, IsExpr b, IsQuery m) => Update a -> (a -> m b) -> m b
  from :: From a -> m a
  where_ :: Expr Bool -> m ()

-- | Name a table, add proper type annotation for type safety.
--   This is helpfyl if you can select your record from several tables or views.
--   Using Table class is safer
table :: (Record a) => ByteString -> From (Rel a)
table bs = From $ do
  nm <- grabName
  let as = raw "\"" <> nm <> raw "\""
      from' = raw "\"" <> raw bs <> raw "\""
  return $ FromTable from' as (Rel as)

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
   withUpdate u f = do
     r <- Query $ do
        (with, expr) <- updateToQuery u
        nm <- grabName
        lift $ tell mempty { queryStateWith = Just $ nm <> raw " AS (" <> with <> raw ")"
                           , queryStateFrom = Just nm }
        return expr
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

-- | from Table instance
recordTable :: forall a . (Table a) => From (Rel a)
recordTable = table $ tableName (Proxy :: Proxy a)

-- | Inner join
innerJoin :: forall a b .(IsExpr a, IsExpr b, Table b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
innerJoin (From mfa) (From mfb) f = From $ do
    fa <- mfa
    fb <- mfb
    let Expr onexpr = f (fromExpr fa) (fromExpr fb)
    return $ FromInnerJoin fa fb onexpr

-- | Cross join
crossJoin :: forall a b .(IsExpr a, IsExpr b, Table b) =>
             From a -> From b -> From (a:.b)
crossJoin (From fa) (From fb) = From $ FromCrossJoin <$> fa <*> fb

-- | Treat query as subquery when joining
-- @
--     x <- fromTable
--     y <- subSelect q
-- @
-- will turn into
-- @
--     SELECT .. FROM x, (SELECT .. from y)
-- @

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
orderBy f = Query . lift $ tell mempty { queryStateOrder = Just $ compileSorting f }

-- | ascend on expression
ascendOn :: (Expr a) -> Sorting
ascendOn (Expr a) = Sorting [a <> raw " DESC"]

-- | descend on expression
descendOn :: Expr t -> Sorting
descendOn (Expr a) = Sorting [a <> raw " ASC"]

-- | set LIMIT
limitTo :: Int -> Query ()
limitTo i = Query . lift $ tell mempty { queryStateLimit = Just i }

-- | set OFFSET
offsetTo :: Int -> Query ()
offsetTo i = Query . lift $ tell mempty { queryStateOffset = Just i }

-- | Execute query and get results
query :: (IsExpr a, FromRow (FromExpr a)) => Connection -> Query a -> IO [FromExpr a]
query c q = PG.query c "?" (Only $ finishQuery q)

-- | Execute discarding results, returns number of rows modified
execute :: Connection -> Query r -> IO Int64
execute c q = PG.execute c "?" (Only $ finishQueryNoRet q)

-- | Format query for previewing
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

-- | Access field of relation
(~>) :: (SingI t) => Rel a -> Field a t b -> Expr b
(Rel r) ~> fld = Expr $ r <> raw "." <> raw "\"" <> raw (fieldColumn fld) <> raw "\""
(RelAliased r) ~> fld = Expr $ raw "\"" <> r <> raw "__" <> raw (fieldColumn fld) <> raw "\""

-- | like operator
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
   withUpdate u f = do
     r <- Inserting $ do
        (with, expr) <- updateToQuery u
        nm <- grabName
        let w = mempty { queryStateWith = Just $ nm <> raw " AS (" <> with <> raw ")"
                       , queryStateFrom = Just nm }
        lift $ tell (mempty { insertWriterFrom = w })
        return expr
     f r

   where_ (Expr r) = Inserting $ do
     lift $ tell mempty {insertWriterFrom = mempty {queryStateWhere = Just r}}
   from f = Inserting $ do
     (r, w) <- prepareFrom f
     lift $ tell (mempty { insertWriterFrom = w })
     return r

instance IsQuery (Updating r) where
   with q f = do
      r <- Updating $ do
         (r, w) <- prepareWith q
         lift $ tell (mempty {insertWriterFrom = w})
         return r
      f r
   withUpdate u f = do
     r <- Updating $ do
        (with, expr) <- updateToQuery u
        nm <- grabName
        let w = mempty { queryStateWith = Just $ nm <> raw " AS (" <> with <> raw ")"
                       , queryStateFrom = Just nm }
        lift $ tell (mempty { insertWriterFrom = w })
        return expr
     f r

   where_ (Expr r) = Updating $ do
     lift $ tell mempty {insertWriterFrom = mempty {queryStateWhere = Just r}}
   from f = Updating $ do
     (r, w) <- prepareFrom f
     lift $ tell (mempty { insertWriterFrom = w })
     return r

instance IsQuery (Deleting r) where
   with q f = do
      r <- Deleting $ do
         (r, w) <- prepareWith q
         lift $ tell w
         return r
      f r
   withUpdate u f = do
     r <- Deleting $ do
        (with, expr) <- updateToQuery u
        nm <- grabName
        let w = mempty { queryStateWith = Just $ nm <> raw " AS (" <> with <> raw ")"
                       , queryStateFrom = Just nm }
        lift $ tell w
        return expr
     f r

   where_ (Expr r) = Deleting $ do
     lift $ tell mempty {queryStateWhere = Just r}
   from f = Deleting $ do
     (r, w) <- prepareFrom f
     lift $ tell w
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

set :: SingI t => Field v t a -> Expr a -> UpdExpr v
set f a = f =. a

setField :: (SingI t, IsUpdate m) => Field v t a -> Expr a -> m v ()
setField f a = setFields (f =. a)

updateTable :: forall a b. Table a => (Rel a -> Updating a b) -> Update b
updateTable f = update (table $ tableName (Proxy :: Proxy a)) f

update :: From (Rel a) -> (Rel a -> Updating a b) -> Update b
update r f = Update $ do
  ns <- get
  let (FromTable nm alias expr, ns') = runState (runFrom r) ns
  put ns'
  compileUpdating (nm <> raw " AS " <> alias) $ f expr

insertIntoTable :: forall a b .(Table a) => (Rel a -> Inserting a b) -> Update b
insertIntoTable f = do
    insert tn f
  where
    tn = table $ tableName (Proxy :: Proxy a)

insert :: From (Rel a) -> (Rel a -> Inserting a b) -> Update b
insert r f = Update $ do
    ns <- get
    let (FromTable nm _ expr, ns') = runState (runFrom r) ns
    put ns'
    compileInserting nm $ f (Rel nm)

delete :: From (Rel a) -> (Rel a -> Deleting a b) -> Update b
delete r f = Update $ do
  ns <- get
  let (FromTable nm alias expr, ns') = runState (runFrom r) ns
  put ns'
  compileDeleting (nm <> raw " AS " <> alias) $ f expr


deleteFromTable :: forall a b .(Table a) => (Rel a -> Deleting a b) -> Update b
deleteFromTable f = delete tn f
  where
    tn = table $ tableName (Proxy :: Proxy a)

queryUpdate :: ((FromExpr t) ~ r, IsExpr t, FromRow r) => Connection -> Update t -> IO [r]
queryUpdate con u = PG.query con "?" (Only $ finishUpdate u)

formatUpdate :: IsExpr t => Connection -> Update t -> IO ByteString
formatUpdate con u = PG.formatQuery con "?" (Only $ finishUpdate u)

executeUpdate :: Connection -> Update a -> IO Int64
executeUpdate con u = PG.execute con "?" (Only $ finishUpdateNoRet u)


updateToQuery :: (MonadState NameSource m, IsExpr t) => Update t -> m (ExprBuilder, t)
updateToQuery (Update mu) = do
  ns <- get
  let ((expr, upd), ns') = runState mu ns
  put ns'
  (proj, expr') <- compileProjection expr
  let res = upd <> raw " RETURNING " <> proj
  return (res, expr')

