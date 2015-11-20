{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
module Database.PostgreSQL.Simple.Dsl.Query
     (
     -- * Example of usage
     -- $use
     -- * Defining Records
      takeField
     , recField
     , RecordParser
     , recordRowParser
     , IsExpr
     -- * Exequting queries
     , IsQuery
     , Query
     , query
     , queryWith
     , execute
     , formatQuery
     -- * From
     , Table (..)
     , FromItem
     , From
     , from
     , table
     , select
     , values
     , pureValues
     , fromValues
     , fromPureValues
     , innerJoin
     , crossJoin
     , Nullable
     , justNullable
     , fromNullable
     , nullableParser
     , leftJoin
     , rightJoin
     , fullJoin
     -- * With
     , with
     , withUpdate
     , Recursive
     , withRecursive
     , union
     , unionAll
     -- * Query
     , where_
     , Sorting
     , orderBy
     , ascendOn
     , descendOn
     , limitTo
     , offsetTo
     , distinct
     , distinctOn
     -- * Updates
     , insert
     , insert'
     , insertFields
     , Inserting
     , Update
     , update
     , setFields
     , UpdExpr
     , Updating
     , delete
     , Deleting
     , returning
     -- * Executing updates
     , queryUpdate
     , queryUpdateWith
     , executeUpdate
     , formatUpdate
     -- * Expressions
     , Expr
     , val
     , val'
     , (==.), (/=.), (<.), (<=.), (>.), (>=.), (||.), (&&.), ( ~.)
     , true, false, just, isNull, isInList
     , not_
     , exists
     , unsafeCast
     , selectExpr
     , cast
     , (+.), (-.)
     -- * Helpers
     , Only(..)
     , (:.)(..)
     -- * Helpers
     , IsRecord
     , FromRecord(..)
     -- * Functions
     , Function
     , function
     , arg
     , call
     -- * aggregation
     , aggregate
     , having
     , groupBy
     , from'
--     , with'
--     , cross
--     , inner
--     , on
     ) where
import           Control.Applicative
import           Control.Exception
import           Control.Monad.State.Class
import           Control.Monad.Trans.State               (runState)
import           Data.Foldable                           (foldlM, toList)

import           Data.ByteString                         (ByteString)
import           Data.ByteString.Builder                 (byteString, char8,
                                                          toLazyByteString)
import           Data.ByteString.Lazy                    (toStrict)
import qualified Data.HashMap.Strict                     as HashMap
import           Data.Int                                (Int64)
import           Data.List                               (intersperse)
import           Data.Maybe
import           Data.Monoid
import           Data.Text                               (Text)
import qualified Data.Text.Encoding                      as T
import qualified Database.PostgreSQL.LibPQ               as PQ
import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          Only (..))

import qualified Database.PostgreSQL.Simple              as PG
import           Database.PostgreSQL.Simple.Dsl.Escaping
import           Database.PostgreSQL.Simple.Dsl.Internal
import qualified Database.PostgreSQL.Simple.Internal     as PG
import           Database.PostgreSQL.Simple.ToField
import qualified Database.PostgreSQL.Simple.Types        as PG
import           GHC.TypeLits                            (KnownSymbol)

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
--      instance Record User whereb
--        data Field User t a where
--          UserKey   :: Field User \"id\" UserId
--          UserLogin :: Field User \"login\" String
--          UserPass  :: Field User \"passwd\" ByteString
--          UserIsDeleted :: Field User \"is_deleted\" Bool
--        recordParser _ = User \<$> fromField UserId'
--                              \<*> fromField UserLogin
--                              \<*> fromField UserPass
--                              \<* takeField UserIsDeleted
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

-- | Name a table, add proper type annotation for type safety.
--   This is helpfyl if you can select your record from several tables or views.
--   Using Table class is safer
--table :: (ParseRecord (Rel a)) => Text -> From (Rel a)
table :: Text -> Table a
table = Table
{-# INLINE table #-}

compileValues  :: (HasNameSource m, IsRecord a) =>  [a] -> a -> m (ExprBuilder, a)
compileValues vals renamed = do
  nm <- grabName
  let res = "(VALUES " <> body <> ") AS " <> namedRow nm renamed
  return $ (res, renamed)
  where
    body = commaSep $ map packRow vals
    packRow r = char8 '(' <> commaSep (asValues r) <> char8 ')'

values :: forall a . (IsRecord a) => [a] -> From a
values inp = From $ do
   renamed <- asRenamed (undefined :: a)
   compileValues inp renamed

pureValues :: forall a. (ToRecord a, IsRecord (AsRecord a)) => [a] -> From (AsRecord a)
pureValues inp = From $ do
   renamed <- asRenamed (undefined :: AsRecord a)
   compileValues (map toRecord inp) renamed

fromValues :: (IsRecord a, IsQuery m) => [a] -> m a
fromValues = from . values

fromPureValues :: (IsRecord (AsRecord a), IsQuery m, ToRecord a) => [a] -> m (AsRecord a)
fromPureValues = from . pureValues

where_ :: IsQuery m => Expr Bool -> m ()
where_ = appendWhereExpr
{-# INLINE where_ #-}

select :: (IsRecord b) => Query b -> From b
select mq = From $ do
  (r, q) <- compIt mq
  nm <- grabName
  renamed <- asRenamed r
  let bld = char8 '(' <> finishIt (asValues r) q <> ") AS "
            <> namedRow nm renamed
  return $ (bld, renamed)

selectExpr :: Query (Expr a) -> Query (Expr a)
selectExpr mq = do
  (r, q) <- compIt mq
  let bld = char8 '(' <> finishIt (asValues r) q <> char8 ')'
  return $ Expr 0 bld
{-
with :: (IsRecord b) => Query b -> (From b -> Query c) -> Query c
with mq act = do
  (e, q) <- compIt mq
  renamed <- asRenamed e
  nm <- grabName
  let bld = namedRow nm renamed
            <> " AS ( " <> finishIt (asValues e) q <> char8 ')'
  appendWith bld
  act $ From . return $ (nm, renamed)
-}
with :: (IsRecord a, IsQuery m) => Query a -> (From a -> m b) -> m b
with mq act = do
  (e, q) <- compIt mq
  renamed <- asRenamed e
  nm <- grabName
  let bld = namedRow nm renamed
            <> " AS ( " <> finishIt (asValues e) q <> char8 ')'
  appendWith bld
  act $ From . return $ (nm, renamed)


withUpdate :: (IsRecord b, IsQuery m) => Update b -> (From b -> m c) -> m c
withUpdate (Update mq) act = do
  (e, q) <- compIt mq
  renamed <- asRenamed e
  nm <- grabName
  let returnings = asValues e
      body = queryAction q <> "\nRETURNING " <> commaSep returnings
      bld = namedRow nm renamed <> " AS ( "  <> body <> char8 ')'
  appendWith bld
  act $ From . return $ (nm, renamed)

from :: (IsQuery m, FromItem f a) => f -> m a
from fr = do
  ns <- getNameSource
  let (frm, ns') = runState (runFrom $ fromItem fr) ns
      (cmp, expr) = frm
  appendFrom cmp
  modifyNameSource (const ns')
  return expr

{-# INLINE from #-}

-- | Inner join
innerJoin :: (FromItem fa a, FromItem fb b) =>
     fa -> fb -> (a -> b -> Expr Bool) -> From (a :. b)
innerJoin fa fb f = From $ do
    (ba, ea) <- runFrom (fromItem fa)
    (bb, eb) <- runFrom (fromItem fb)
    let Expr _ onexpr = f ea eb
        bres = ba <> " INNER JOIN " <> bb <> " ON " <> onexpr
    return (bres, ea :. eb)

crossJoin :: From a -> From b -> From (a :. b)
crossJoin (From mfa) (From mfb) = From $ do
    (ba, ea) <- mfa
    (bb, eb) <- mfb
    let bres = addParens (ba <> " CROSS JOIN " <> addParens bb)
    return (bres, ea :. eb)

leftJoin :: (FromItem fa a, FromItem fb b)
         => fa -> fb -> (a -> b -> Expr Bool)
         -> From (a :. Nullable b)
leftJoin fa fb f = From $ do
    (ba, ea) <- runFrom (fromItem fa)
    (bb, eb) <- runFrom (fromItem fb)
    let Expr _ onexpr = f ea eb
        bres = addParens (ba <> " LEFT JOIN " <> bb <> " ON " <> onexpr)
    return (bres, ea :. Nullable eb)

rightJoin :: (FromItem fa a, FromItem fb b)
         => fa -> fb -> (a -> b -> Expr Bool)
         -> From (Nullable a :. b)
rightJoin fa fb f = From $ do
    (ba, ea) <- runFrom (fromItem fa)
    (bb, eb) <- runFrom (fromItem fb)
    let Expr _ onexpr = f ea eb
        bres = addParens (ba <> " RIGHT JOIN " <> addParens bb <> " ON " <> onexpr)
    return (bres, Nullable ea :. eb)

fullJoin :: (FromItem fa a, FromItem fb b)
         => fa -> fb -> (a -> b -> Expr Bool)
         -> From (Nullable a :. Nullable b)
fullJoin fa fb f = From $ do
    (ba, ea) <- runFrom (fromItem fa)
    (bb, eb) <- runFrom (fromItem fb)
    let Expr _ onexpr = f ea eb
        bres = addParens (ba <> " FULL JOIN " <> addParens bb <> " ON " <> onexpr)
    return (bres, Nullable ea :. Nullable eb)

exists :: (IsQuery m) => Query (Expr a) -> m (Expr Bool)
exists mq = do
    (r, q') <- compIt mq
    let body = finishIt (asValues r) q'
    return . term $ " EXISTS (" <> body <> char8 ')'

-- | append to ORDER BY clase
orderBy :: Sorting -> Query ()
orderBy f | sortingEmpty f = return ()
          | otherwise = modify $ \st -> let or' = queryStateOrder st
                                        in st { queryStateOrder = go or' }
   where go o = o <> Just f

-- | ascend on expression
ascendOn :: (Expr a) -> Sorting
ascendOn (Expr _ a) = Sorting [(a, " ASC")]

-- | descend on expression
descendOn :: Expr t -> Sorting
descendOn (Expr _ a) = Sorting [(a, " DESC")]

-- | set LIMIT
limitTo :: Int -> Query ()
limitTo = modifyLimit . const . Just
-- | set OFFSET
offsetTo :: Int -> Query ()
offsetTo = modifyOffset . const . Just

distinct :: Query ()
distinct = modifyDistinct $ const Distinct

distinctOn :: Sorting -> Query ()
distinctOn s = do
    modifyDistinct . const $ DistinctOn s

-- | Execute query and get results
queryWith :: Connection -> (a -> RecordParser r) -> Query a -> IO [r]
queryWith c fromRec q = PG.queryWith_ parser c ( PG.Query $ buildQuery body)
  where
    (body, parser) = finishQuery fromRec q

query :: FromRecord a b => Connection -> Query a -> IO [b]
query c q = queryWith c fromRecord q

-- | Execute discarding results, returns number of rows modified
execute :: Connection -> Query r -> IO Int64
execute c q = PG.execute_ c . PG.Query $ buildQuery (finishQueryNoRet q)

-- | Format query for previewing
formatQuery :: IsRecord a => Query a -> ByteString
formatQuery q = buildQuery $ compileQuery q


-- | lift value to Expr
val :: (ToField a ) => a -> Expr a
val = term . rawField

val' :: (ToField a, IsExpr expr) => a -> expr a
val' = fromExpr . term . rawField

infixl 6 +., -.

(-.) :: IsExpr expr => expr a -> expr a -> expr a
a -. b = binOp 6 (char8 '-') a b
(+.) :: IsExpr expr => expr a -> expr a -> expr a
a +. b = binOp 6 (char8 '+') a b

infix 4 ==., /=., <., <=., >., >=.

(==.) :: IsExpr expr => expr a -> expr a -> expr Bool
a ==. b = binOp 15 (char8 '=') a b

(/=.) :: IsExpr expr => expr a -> expr a -> expr Bool
a /=. b = binOp 15 "!=" a b

(>.) :: IsExpr expr => expr a -> expr a -> expr Bool
a >. b = binOp 15 (char8 '>') a b

(>=.) :: IsExpr expr => expr a -> expr a -> expr Bool
a >=. b = binOp 15 ">=" a b

(<.) :: IsExpr expr => expr a -> expr a -> expr Bool
a <. b = binOp 15 (char8 '<') a b

(<=.) :: IsExpr expr => expr a -> expr a -> expr Bool
a <=. b = binOp 15 "<=" a b

infixr 3 &&.
(&&.), (||.) :: IsExpr expr => expr Bool -> expr Bool -> expr Bool
a &&. b = binOp 18 " AND " a b

infixr 2 ||.
a ||. b = binOp 19 " OR " a b

-- | Access field of relation
not_ :: IsExpr expr => expr Bool -> expr Bool
not_ r = prefOp 17 " NOT " r

-- | like operator
(~.) :: Expr Text -> Expr Text -> Expr Bool
(~.) a b = binOp 14 " ~ " a b

isInList :: (IsExpr expr, ToField a) => expr a -> [a] -> expr Bool
isInList _ [] = false
isInList a l = binOp 11 " IN " a (fromExpr $ term (lst))
  where
    lst = escapeAction $ toField $ PG.In l

true :: IsExpr expr => expr Bool
true = fromExpr $ term "true"
{-# INLINE true #-}

false :: IsExpr expr => expr Bool
false = fromExpr $ term "false"
{-# INLINE false #-}

just :: Expr a -> Expr (Maybe a)
just (Expr p r) = Expr p r

isNull :: Expr a -> Expr Bool
isNull r = binOp 7 " IS " r (term "NULL")

unsafeCast :: Expr a -> Expr b
unsafeCast (Expr p r) = Expr p r

cast :: IsExpr expr => ByteString -> expr a -> expr b
cast t eb = fromExpr $ Expr 1 $ parenPrec (1 < p) b <> "::" <> byteString t
  where
    (Expr p b) = toExpr eb
{-# INLINE cast #-}

setFields :: UpdExpr t -> Updating t ()
setFields (UpdExpr x) = Updating $ do
  st <- get
  put st { queryAction = queryAction st <> x }

insertFields :: UpdExpr t -> Inserting t ()
insertFields (UpdExpr x) = Inserting $ do
  st <- get
  put st { queryAction = queryAction st <> x }


-- | Same as above but table is taken from table class
-- > deleteFromTable $ \u -> where & u~>UserKey ==. val uid
queryUpdateWith :: Connection -> (a -> RecordParser b) -> Update a -> IO [b]
queryUpdateWith con fromRec u = do
    let q = buildQuery body
    PG.queryWith_ parser con (PG.Query q)
  where
    (body, parser) = finishUpdate fromRec u

queryUpdate :: FromRecord a b => Connection -> Update a -> IO [b]
queryUpdate con u = queryUpdateWith con fromRecord u
{-# INLINE queryUpdate #-}

formatUpdate :: IsRecord t => Update t -> ByteString
formatUpdate u = buildQuery (compileUpdate u)

executeUpdate :: Connection -> Update a -> IO Int64
executeUpdate con u = PG.execute_ con $ PG.Query ( buildQuery (finishUpdateNoRet u))

withRecursive :: (IsRecord a) =>  Recursive a -> (From a -> Query b) -> Query b
withRecursive (Recursive un q f) ff = compileUnion un q f >>= ff

union :: Query a -> (From a -> Query a) -> Recursive a
union = Recursive UnionDistinct

unionAll :: Query a -> (From a -> Query a) -> Recursive a
unionAll = Recursive UnionAll

compileUnion :: (IsRecord a) => Union -> Query a -> (From a -> Query a) -> Query (From a)
compileUnion un q f = do
   nm <- grabName
   (exprNonRec, queryNonRec) <- compIt q
   renamed <- asRenamed exprNonRec
   let frm = From . return $ (nm, renamed)
   (exprRec, queryRec) <- compIt $ f frm
   let bld = namedRow nm renamed
           <> " AS (" <> finishIt (asValues exprNonRec) queryNonRec
           <> unioner <> finishIt (asValues exprRec) queryRec <> char8 ')'
   modifyRecursive $ const True
   appendWith bld
   return frm
   where
     unioner = case un of
         UnionDistinct -> " UNION "
         UnionAll      -> " UNION ALL "
{-# INLINE compileUnion #-}
{-
aggregate :: (IsRecord a, IsAggregate b) => (a -> b) -> Query a -> Query (AggRecord b)
aggregate f q = do
    (r, q') <- compIt q
    let aggrExpr = f r
        resExpr = fromAggr aggrExpr
    let compiled = case compileGroupBy aggrExpr of
                [] -> finishIt (asValues resExpr) q'
                xs -> finishItAgg (asValues resExpr) (Just $ commaSep xs) q'
    nm <- newName
    renamed <- asRenamed resExpr
    appendFrom $ (plain "(" `D.cons` (compiled `D.snoc` plain ") AS ")) <> namedRow nm renamed
    return renamed
-}
aggregate :: IsAggregate b => (a -> Aggregator b) -> Query a -> Query (AggRecord b)
aggregate f q = do
    r <- q
    let Aggregator aggQ = f r
    fmap fromAggr aggQ

groupBy :: Expr a -> Aggregator (ExprA a)
groupBy e@(Expr _ b) = do
  appendGroupBy b
  return $ ExprA e

having :: ExprA Bool -> Aggregator ()
having (ExprA (Expr _ a)) = appendHaving a

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

escapeIdentifier :: Connection -> ByteString -> IO ByteString
escapeIdentifier conn s =
    PG.withConnection conn $ \c ->
    PQ.escapeIdentifier c s >>= checkError c

buildQuery :: ExprBuilder -> ByteString
buildQuery = toStrict . toLazyByteString
{-
cross :: (IsRecord a, IsRecord b) => From a -> From b -> From (a:.b)
cross = crossJoin
-}
on :: (( a-> b-> Expr Bool) -> From (a:.b)) -> (a -> b -> Expr Bool) -> From (a:.b)
on f b = f b
{-
inner :: forall a b .(IsRecord a, IsRecord b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
inner = innerJoin
-}
--infixr 3 `inner`
infixl 2 `on`

--infixr 3 `cross`

from' :: (IsQuery m, FromItem f a) => f -> (a -> m b) -> m b
from' fi f = do
  ns <- grabNS
  let (frm, ns') = runState (runFrom $ fromItem fi) ns
      (cmp, expr) = frm
  appendFrom cmp
  modifyNS $ const ns'
  r <- f expr
  return r

{-# INLINE from' #-}

exists' :: IsRecord a => Query a -> Expr a
exists' mq = term $ " EXISTS (" <> body <> char8 ')'
  where
    body = compileQuery mq

returning :: (t -> a) -> Update t -> Update a
returning f (Update m) = Update $ do
  expr <- m
  return $ f expr

