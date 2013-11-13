{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
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
     , recordTable
     , values
     , pureValues
     , subQuery
     , fromTable
     , fromValues
     , fromPureValues
     , fromSubQuery
     , innerJoin
     , crossJoin
     , subSelect
     -- * With
     , with
     , withUpdate
     , Recursive
     , withRecursive
     , union
     , unionAll
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
     , insert
     , insertIntoTable
     , returning
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
     , Rel, (~>), (?>)
     , Expr
     , val
     , (==.), (<.), (<=.), (>.), (>=.), (||.), (&&.), ( ~.)
     , true, false, just, isNull, isInList
     , whole
     , only
     , unonly
     , not_
     , exists
     , unsafeCast
     , array
     , array_append
     , array_prepend
     , array_cat
     -- * Helpers
     , Whole(..)
     , Only(..)
     , (:.)(..)
     -- * Helpers
     , IsRecord
     , FromRecord
     -- * Functions
     , Function
     , function
     , arg
     , call
     -- * aggregation
     , aggregate
     , sum_
     , count
     , countAll
     , groupBy
     ) where
import           Control.Applicative
import           Control.Exception
import           Control.Monad.State.Class
import           Control.Monad.Trans
import           Control.Monad.Trans.State               (runState)
import           Control.Monad.Trans.Writer

import           Blaze.ByteString.Builder                as B
import           Data.ByteString                         (ByteString)
import qualified Data.DList                              as D
import           Data.Foldable                           (foldlM)
import           Data.Int                                (Int64)
import           Data.List                               (intersperse)
import           Data.Maybe
import           Data.Monoid
import           Data.Proxy                              (Proxy (..))
import           Data.Text                               (Text)
import           Data.Vector                             (Vector)
import qualified Database.PostgreSQL.LibPQ               as PQ
import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          FromRow, Only (..))
import qualified Database.PostgreSQL.Simple              as PG
import           Database.PostgreSQL.Simple.Dsl.Internal
import qualified Database.PostgreSQL.Simple.Internal     as PG
import           Database.PostgreSQL.Simple.ToField
import qualified Database.PostgreSQL.Simple.Types        as PG
import           GHC.TypeLits                            (SingI)

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
  with :: (IsRecord a) => Query a -> (From a -> m b) -> m b
  withUpdate  :: (IsRecord a) => Update a -> (From a -> m b) -> m b
  from :: From a -> m a
  where_ :: Expr Bool -> m ()

-- | Name a table, add proper type annotation for type safety.
--   This is helpfyl if you can select your record from several tables or views.
--   Using Table class is safer
table :: (Record a) => ByteString -> From (Rel a)
table bs = From $ do
  let nameBld = B.fromByteString bs
  alias <- grabAlias nameBld
  return $ FromTable nameBld alias (Rel alias)


compileValues :: (MonadState NameSource m, IsRecord a, IsRecord a1) =>
     [a] -> a1 -> m (FromD a1)
compileValues vals renamed = do
  nm <- grabName
  let res = raw "(VALUES "<> body <> raw ") AS "<> namedRow nm renamed
  return $ FromQuery res renamed
  where
    body = commaSep $ map packRow vals
    packRow r = raw "(" <> commaSep (asValues r) <> raw ")"

values :: forall a . (IsRecord a) => [a] -> From a
values inp = From $ do
   renamed <- asRenamed (undefined :: a)
   compileValues inp renamed

pureValues :: forall a. (ToRecord a, IsRecord (AsRecord a)) => [a] -> From (AsRecord a)
pureValues inp = From $ do
   renamed <- asRenamed (undefined :: AsRecord a)
   compileValues (map toRecord inp) renamed

subQuery :: IsRecord a => Query a -> From a
subQuery mq = From $ do
   (r, q') <- compIt mq
   renamed <- asRenamed r
   nm <- grabName
   let res = raw "(" <> finishIt (asValues r) q' <> raw ") AS" <> namedRow nm renamed
   return $ FromQuery res renamed

-- | Start query with table - SELECT table.*
fromTable :: forall a.(Table a, Record a) => Query (Rel a)
fromTable = from . table $ tableName (Proxy :: Proxy a)


fromValues :: (IsRecord a, IsQuery m) => [a] -> m a
fromValues = from . values

fromPureValues :: (IsRecord (AsRecord a), IsQuery m, ToRecord a) => [a] -> m (AsRecord a)
fromPureValues = from . pureValues

fromSubQuery :: (IsQuery m, IsRecord a) => Query a -> m a
fromSubQuery = from . subQuery

instance IsQuery Query where
--   with :: (IsRecord a, IsRecord b) => Query a -> (a -> Query b) -> Query b
   with q f = do
      r <- Query $ do
         (r, nm, w) <- prepareWith q
         lift $ tell w
         return $ From (return $ FromQuery nm r)
      f r
   withUpdate u f = do
     r <- Query $ do
        (withQ, expr) <- updateToQuery u
        nm <- grabName
        renamed <- asRenamed expr
        lift $ tell mempty { queryStateWith = Just $ namedRow nm renamed
                                                   <> raw " AS (" <> withQ <> raw ")"
                           }
        return $ From (return $ FromQuery nm renamed)
     f r

   where_ (Expr r) = Query $ do
     lift $ tell mempty {queryStateWhere = Just r}
   from f = Query $ do
     (r, w) <- prepareFrom f
     lift $ tell w
     return r

prepareWith :: (MonadState NameSource m, IsRecord t) => Query t
            -> m (t, ExprBuilder, QueryState)
prepareWith q = do
   (r, q') <- compIt q
   nm <- grabName
   renamed <- asRenamed r
   let bld = namedRow nm renamed <> raw " AS ("
                <> finishIt (asValues r) q' <> raw ")"
   return (renamed, nm, mempty { queryStateWith = Just bld })

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
innerJoin :: forall a b .(IsRecord a, IsRecord b, Table b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
innerJoin (From mfa) (From mfb) f = From $ do
    fa <- mfa
    fb <- mfb
    let Expr onexpr = f (fromExpr fa) (fromExpr fb)
    return $ FromInnerJoin fa fb onexpr

-- | Cross join
crossJoin :: forall a b .(IsRecord a, IsRecord b, Table b) =>
             From a -> From b -> From (a:.b)
crossJoin (From fa) (From fb) = From $ FromCrossJoin <$> fa <*> fb

-- | Treat query as subquery when joining
-- useful to force evaluation of expressions, othervise volatile
-- functions will be called every time they are encountered in any expression.
--
-- >     x <- fromTable
-- >     y <- subSelect q
--
--   will turn into
--
-- >    SELECT .. FROM x, (SELECT .. from y)
--
--

namedRow :: IsRecord a => ExprBuilder -> a -> ExprBuilder
namedRow nm r = nm <> raw "(" <> commaSep (asValues r) <> raw ")"

subSelect :: (IsRecord a) => Query a -> Query a
subSelect mq = Query $ do
  (r, q) <- compIt mq
  lift $ tell mempty { queryStateWith = queryStateWith q }
  nm <- grabName
  renamed <- asRenamed r
  let cleanQ = q { queryStateWith = Nothing }
      bld = raw "(" <> finishIt (asValues r) cleanQ <> raw ") AS " <> namedRow nm renamed
  lift $ tell (mempty { queryStateFrom = Just bld})
  return renamed

exists :: IsRecord a => Query a -> Query (Expr Bool)
exists mq = Query $ do
  (r, q) <- compIt mq
  lift $ tell mempty { queryStateWith = queryStateWith q }
  let res = raw " EXISTS (" <> finishIt (asValues r) q <> raw ")"
  return $ Expr res

-- | append to ORDER BY clase
orderBy :: Sorting -> Query ()
orderBy f = Query . lift $ tell mempty { queryStateOrder = Just $ compileSorting f }

-- | ascend on expression
ascendOn :: (Expr a) -> Sorting
ascendOn (Expr a) = Sorting [a <> raw " ASC"]

-- | descend on expression
descendOn :: Expr t -> Sorting
descendOn (Expr a) = Sorting [a <> raw " DESC"]

-- | set LIMIT
limitTo :: Int -> Query ()
limitTo i = Query . lift $ tell mempty { queryStateLimit = Just i }

-- | set OFFSET
offsetTo :: Int -> Query ()
offsetTo i = Query . lift $ tell mempty { queryStateOffset = Just i }

-- | Execute query and get results
query :: (IsRecord a, FromRow (FromRecord a)) => Connection -> Query a -> IO [FromRecord a]
query c q = buildQuery c (finishQuery q) >>= PG.query_ c . PG.Query

-- | Execute discarding results, returns number of rows modified
execute :: Connection -> Query r -> IO Int64
execute c q = PG.execute c "?" (Only $ finishQueryNoRet q)

-- | Format query for previewing
formatQuery :: IsRecord a => Connection -> Query a -> IO ByteString
formatQuery c q = buildQuery c (finishQuery q)

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
not_ :: Expr Bool -> Expr Bool
not_ (Expr r) = Expr $ raw "NOT "<> r
mkAcc :: SingI t => Rel r -> Field v t a -> Expr b
mkAcc rel fld = Expr $ mkAccess rel (fieldColumn fld)
infixl 9 ~>, ?>
(~>) :: (SingI t) => Rel a -> Field a t b -> Expr b
(~>) = mkAcc

(?>) :: (SingI t) => Rel (Maybe a) -> Field a t b -> Expr (Maybe b)
(?>) = mkAcc

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

unonly :: Expr (Only a) -> Expr a
unonly (Expr a) = Expr a

true :: Expr Bool
true = Expr $ raw "true"

false :: Expr Bool
false = Expr $ raw "true"

just :: Expr a -> Expr (Maybe a)
just (Expr r) = Expr r

isNull :: Expr a -> Expr Bool
isNull (Expr r) = Expr $ addParens r <> raw " IS NULL"

unsafeCast :: Expr a -> Expr b
unsafeCast (Expr r) = Expr r


array :: Expr a -> Expr (Vector a)
array (Expr r) = Expr $ raw "ARRAY["<> r <> raw "]"

array_append :: Expr (Vector a) -> Expr a -> Expr (Vector a)
array_append (Expr arr) (Expr v) = Expr $ raw "array_append("
             <> arr <> raw "," <> v <> raw ")"

array_prepend :: Expr a -> Expr (Vector a) -> Expr (Vector a)
array_prepend (Expr v) (Expr arr) = Expr $
  raw "array_prepend(" <> v <> raw "," <> arr <> raw ")"
array_cat :: Expr (Vector a) -> Expr (Vector a) -> Expr (Vector a)
array_cat (Expr a) (Expr b) = Expr $
  raw "array_cat(" <> a <> raw "," <> b <> raw ")"

instance IsQuery (Updating r) where
   with q f = do
      r <- Updating $ do
         (r, nm, w) <- prepareWith q
         lift $ tell (mempty {insertWriterFrom = w})
         return $ From (return $ FromQuery nm r)
      f r
   withUpdate u f = do
     r <- Updating $ do
        (withQ, expr) <- updateToQuery u
        nm <- grabName
        let w = mempty { queryStateWith = Just $ nm <> raw " AS (" <> withQ <> raw ")"
                       }
        lift $ tell (mempty { insertWriterFrom = w })
        return $ From (return $ FromQuery nm expr)
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
         (r, nm, w) <- prepareWith q
         lift $ tell w
         return $ From (return $ FromQuery nm r)
      f r
   withUpdate u f = do
     r <- Deleting $ do
        (withQ, expr) <- updateToQuery u
        nm <- grabName
        let w = mempty { queryStateWith = Just $ nm <> raw " AS (" <> withQ <> raw ")"
                        }
        lift $ tell w
        return $ From (return $ FromQuery nm expr)
     f r

   where_ (Expr r) = Deleting $ do
     lift $ tell mempty {queryStateWhere = Just r}
   from f = Deleting $ do
     (r, w) <- prepareFrom f
     lift $ tell w
     return r

infixr 7 =., =.!
(=.) :: forall v t a. (SingI t) => Field v t a -> Expr a -> UpdExpr v
f =. (Expr a) = UpdExpr [(fieldColumn f, a)]

(=.!) :: (SingI t, ToField a) => Field v t a -> a -> UpdExpr v
(=.!) f e = f =. val e

set :: SingI t => Field v t a -> Expr a -> UpdExpr v
set f a = f =. a

setField :: (SingI t) => Field v t a -> Expr a -> Updating v ()
setField f a = setFields (f =. a)

setFields :: UpdExpr t -> Updating t ()
setFields (UpdExpr x) = Updating . lift $ tell (mempty { insertWriterSets = x })


returning :: (t -> a) -> Update t -> Update a
returning f (Update m) = Update $ do
  (expr,bld) <- m
  return $ (f expr, bld)

updateTable :: forall a b. Table a => (Rel a -> Updating a b) -> Update b
updateTable f = update (table $ tableName (Proxy :: Proxy a)) f

update :: From (Rel a) -> (Rel a -> Updating a b) -> Update b
update r f = Update $ do
  ns <- get
  let (FromTable nm alias expr, ns') = runState (runFrom r) ns
  put ns'
  compileUpdating (builder $ nm <> B.fromByteString " AS " <> alias) $ f expr

insert :: From (Rel a) -> Query (UpdExpr a) -> Update (Rel a)
insert r mq = Update $ do
  ns <- get
  let (FromTable nm _ _, ns') = runState (runFrom r) ns
      rel = Rel nm
  put ns'
  (UpdExpr upd, q) <- compIt mq
  let inserting = Inserting $ do
        lift $ tell $ InsertWriter q upd
        return rel
  compileInserting (builder nm) $ inserting


insertIntoTable :: forall a .(Table a) => Query (UpdExpr a) -> Update (Rel a)
insertIntoTable f = do
    insert tn f
  where
    tn = table $ tableName (Proxy :: Proxy a)

-- | Delete row from given table
-- > delete (from users) $ \u -> where & u~>UserKey ==. val uid
-- turns into
-- > DELETE FROM users WHERE id ==. 42
delete :: From (Rel a) -> (Rel a -> Deleting a b) -> Update b
delete r f = Update $ do
  ns <- get
  let (FromTable nm alias expr, ns') = runState (runFrom r) ns
  put ns'
  compileDeleting (builder $ nm <> B.fromByteString " AS " <> alias) $ f expr

-- | Same as above but table is taken from table class
-- > deleteFromTable $ \u -> where & u~>UserKey ==. val uid
deleteFromTable :: forall a b .(Table a) => (Rel a -> Deleting a b) -> Update b
deleteFromTable f = delete tn f
  where
    tn = table $ tableName (Proxy :: Proxy a)

queryUpdate :: ((FromRecord t) ~ r, IsRecord t, FromRow r) => Connection -> Update t -> IO [r]
queryUpdate con u = PG.query con "?" (Only $ finishUpdate u)

formatUpdate :: IsRecord t => Connection -> Update t -> IO ByteString
formatUpdate con u = PG.formatQuery con "?" (Only $ finishUpdate u)

executeUpdate :: Connection -> Update a -> IO Int64
executeUpdate con u = PG.execute con "?" (Only $ finishUpdateNoRet u)


updateToQuery :: (MonadState NameSource m, IsRecord t) => Update t -> m (ExprBuilder, t)
updateToQuery (Update mu) = do
  ns <- get
  let ((expr, upd), ns') = runState mu ns
  put ns'
  let res = upd <> raw " RETURNING " <> commaSep (asValues expr)
  return (res, expr)


withRecursive :: (IsRecord a) =>  Recursive a -> (a -> Query b) -> Query b
withRecursive (Recursive un q f) ff = compileUnion un q f >>= ff

union :: Query a -> (a -> Query a) -> Recursive a
union = Recursive UnionDistinct

unionAll :: Query a -> (a -> Query a) -> Recursive a
unionAll = Recursive UnionAll

compileUnion :: (IsRecord a) => Union -> Query a -> (a -> Query a) -> Query a
compileUnion un q f = Query $ do
   nm <- grabName
   (exprNonRec, queryNonRec) <- compIt q
   renamed <- asRenamed exprNonRec
   let (Query recurs) = f renamed
   (exprRec, queryRec) <- compIt . Query $ do
     lift $ tell mempty {queryStateFrom = Just nm}
     recurs
   let bld = namedRow nm renamed <> raw " AS (" <> finishIt (asValues exprNonRec) queryNonRec
           <> unioner <> finishIt (asValues exprRec) queryRec <> raw " )"
   lift $ tell mempty { queryStateFrom = Just nm, queryStateRecursive = Any True
                      , queryStateWith = Just bld }
   return renamed
   where
     unioner = case un of
         UnionDistinct -> raw " UNION "
         UnionAll      -> raw " UNION ALL "
{-# INLINE compileUnion #-}

aggregate :: (IsRecord a, IsAggregate b) =>
     (a -> b) -> Query a -> Query (AggRecord b)
aggregate f q = do
  Query $ do
    (r, q') <- compIt q
    let aggrExpr = f r
        resExpr = fromAggr aggrExpr
    let compiled = case compileGroupBy aggrExpr of
                [] -> finishIt (asValues resExpr) q'
                xs -> finishItAgg (asValues resExpr) (Just $ commaSep xs) q'
    nm <- grabName
    renamed <- asRenamed resExpr
    lift $ tell mempty { queryStateFrom = Just $ raw "(" <> compiled <> raw ") AS "
                                                 <> namedRow nm renamed }
    return renamed

groupBy :: Expr a -> ExprA a
groupBy (Expr a) = ExprGrp a

sum_ :: Num a => Expr a -> ExprA Int64
sum_ (Expr a) = ExprAgg (raw "sum(" <> a <> raw ")")

countAll :: ExprA Int64
countAll = ExprAgg (raw "count(*)")

count :: Expr a -> ExprA Int64
count (Expr a) = ExprAgg $ raw "count(" <> a <> raw ")"


checkError :: PQ.Connection -> Maybe a -> IO a
checkError _ (Just x) = return x
checkError c Nothing  = PQ.errorMessage c >>= throwIO . PG.fatalError . fromMaybe "FatalError"

escapeStringConn :: Connection -> ByteString -> IO ByteString
escapeStringConn conn s =
    PG.withConnection conn $ \c ->
    PQ.escapeStringConn c s >>= checkError c

escapeByteaConn :: Connection -> ByteString -> IO ByteString
escapeByteaConn conn s =
    PG.withConnection conn $ \c ->
    PQ.escapeByteaConn c s >>= checkError c

buildQuery :: Connection -> [Action] -> IO ByteString
buildQuery conn q = B.toByteString <$> foldlM sub mempty q
  where
    sub acc (Plain b)       = pure $ acc <> b
    sub acc (Escape s)      = (mappend acc . quote) <$> escapeStringConn conn s
    sub acc (EscapeByteA s) = (mappend acc . quote) <$> escapeByteaConn conn s
    sub acc (Many xs)       = foldlM sub acc xs
    quote = inQuotes . B.fromByteString
