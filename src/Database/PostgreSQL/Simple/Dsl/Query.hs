{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
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
       Record (..)
     , takeField
     , recField
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
--     , rtable
     , select
     , values
     , pureValues
     , fromTable
     , fromValues
     , fromPureValues
     , innerJoin
     , crossJoin
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
     , Update
     , UpdExpr
     , Updating
     , update
     , updateTable
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
     , Rel, (~>), (?>)
     , Expr
     , val
     , (==.), (<.), (<=.), (>.), (>=.), (||.), (&&.), ( ~.)
     , true, false, just, isNull, isInList
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
     , sum_
     , count
     , countAll
     , groupBy
     , from'
     , with'
     , cross
     , inner
--     , on
     ) where
import           Control.Applicative
import           Control.Exception
import           Control.Monad                           (unless)
import           Control.Monad.State.Class
import           Control.Monad.Trans
import           Control.Monad.Trans.State               (runState)
import           Control.Monad.Trans.Writer
import           Data.Foldable                           (foldlM, toList)

import           Blaze.ByteString.Builder                as B
import           Data.ByteString                         (ByteString)
import qualified Data.ByteString.Char8                   as B
import qualified Data.DList                              as D
import           Data.Int                                (Int64)
import           Data.List                               (intersperse)
import           Data.Maybe
import           Data.Monoid
import           Data.Proxy                              (Proxy (..))
import           Data.Sequence                           (Seq, (<|), (|>))
import qualified Data.Sequence                           as Seq
import           Data.Text                               (Text)
import qualified Data.Text.Encoding                      as T
import           Data.Vector                             (Vector)
import qualified Database.PostgreSQL.LibPQ               as PQ
import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          Only (..))
import qualified Database.PostgreSQL.Simple              as PG
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
table :: (Record a) => Text -> From (Rel a)
table nm = From $ do
  let bs = T.encodeUtf8 nm
      nameBld = B.fromByteString bs
  alias <- newName_ --grabAlias nameBld
  return $ FromTable bs alias (RelTable alias)

compileValues  :: (HasNameSource m, IsRecord a) =>  [a] -> a -> m (FromD a)
compileValues vals renamed = do
  nm <- newName
  let res = ((plain "(VALUES "<| body) |> plain ") AS ") <> (exprToSeq $ namedRow nm renamed)
  return $ FromQuery res renamed
  where
    body = exprToSeq $ commaSep $ map packRow vals
    packRow r = raw "(" <> commaSep (asValues r) <> raw ")"

values :: forall a . (IsRecord a) => [a] -> From a
values inp = From $ do
   renamed <- asRenamed (undefined :: a)
   compileValues inp renamed

pureValues :: forall a. (ToRecord a, IsRecord (AsRecord a)) => [a] -> From (AsRecord a)
pureValues inp = From $ do
   renamed <- asRenamed (undefined :: AsRecord a)
   compileValues (map toRecord inp) renamed

-- | Start query with table - SELECT table.*
fromTable :: forall a.(Table a, Record a) => Query (Rel a)
fromTable = from . table $ tableName (Proxy :: Proxy a)


fromValues :: (IsRecord a, IsQuery m) => [a] -> m a
fromValues = from . values

fromPureValues :: (IsRecord (AsRecord a), IsQuery m, ToRecord a) => [a] -> m (AsRecord a)
fromPureValues = from . pureValues

where_ :: IsQuery m => Expr Bool -> m ()
where_ = appendWhereExpr


select :: (IsRecord b) => Query b -> From b
select mq = From $ do
  (r, q) <- compIt mq
  nm <- newName
  renamed <- asRenamed r
  let bld = ((plain "(" <| finishIt (asValues r) q) |> plain ") AS ")
            <> exprToSeq (namedRow nm renamed)
  return $ FromQuery bld renamed

with :: (IsRecord b) => Query b -> (From b -> Query c) -> Query c
with mq act = do
  (e, q) <- compIt mq
  renamed <- asRenamed e
  nm <- newName
  let bld = exprToSeq (namedRow nm renamed)
            <> (plain " AS ( " <| (finishIt (asValues e) q |> plain ")"))
  appendWith bld
  act $ From . return $ FromQuery (exprToSeq nm) renamed

withUpdate (Update mq) = do
  (e, q) <- compIt mq
  renamed <- asRenamed e
  nm <- newName
  let bld = exprToSeq (namedRow nm renamed)
          <> (plain " AS ( "  <| (queryAction q |> plain ")"))
  appendWith bld
  return renamed

from :: IsQuery m => From a -> m a
from (From mfrm) = do
  ns <- getNameSource
  let (frm, ns') = runState mfrm ns
      (cmp, expr) = compileFrom frm
  appendFrom cmp
  modifyNameSource (const ns')
  return expr

-- | Inner join
innerJoin :: forall a b .(IsRecord a, IsRecord b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
innerJoin (From mfa) (From mfb) f = From $ do
    fa <- mfa
    fb <- mfb
    let Expr _ onexpr = f (fromExpr fa) (fromExpr fb)
    return $ FromInnerJoin fa fb onexpr

-- | Cross join
crossJoin :: forall a b .(IsRecord a, IsRecord b) =>
             From a -> From b -> From (a:.b)
crossJoin (From fa) (From fb) = From $ FromCrossJoin <$> fa <*> fb


namedRow :: IsRecord a => ExprBuilder -> a -> ExprBuilder
namedRow nm r = nm <> raw "(" <> commaSep (asValues r) <> raw ")"


exists :: IsRecord a => Query a -> Expr Bool
exists mq = term $ D.fromList . toList $ (plain " EXISTS (" <| body) |> plain ")"
  where
    body = compileQuery mq

-- | append to ORDER BY clase
orderBy :: Sorting -> Query ()
orderBy f | sortingEmpty f = return ()
          | otherwise = modify $ \st -> let or = queryStateOrder st
                                        in st { queryStateOrder = go or }
   where go o | Seq.null o = sort
              | otherwise = (o |> plain ",") <> sort
         sort = compileSorting f

-- | ascend on expression
ascendOn :: (Expr a) -> Sorting
ascendOn (Expr _ a) = Sorting [a <> raw " ASC"]

-- | descend on expression
descendOn :: Expr t -> Sorting
descendOn (Expr _ a) = Sorting [a <> raw " DESC"]

-- | set LIMIT
limitTo :: Int -> Query ()
limitTo = modifyOffset . const . Just
-- | set OFFSET
offsetTo :: Int -> Query ()
offsetTo = modifyLimit . const . Just

distinct :: Query ()
distinct = modifyDistinct $ const Distinct

distinctOn :: Expr t -> Query ()
distinctOn (Expr _ e) = modifyDistinct . const $ DistinctOn e

-- | Execute query and get results
query :: forall a b . (FromRecord a b, IsRecord a) => Connection -> Query a -> IO [b]
query c q = buildQuery c body >>= PG.queryWith_ parser c . PG.Query
  where
    (body, parser) = finishQuery q

-- | Execute discarding results, returns number of rows modified
execute :: Connection -> Query r -> IO Int64
execute c q = PG.execute_ c . PG.Query =<< buildQuery c (finishQueryNoRet q)

-- | Format query for previewing
formatQuery :: IsRecord a => Connection -> Query a -> IO ByteString
formatQuery c q = buildQuery c $ compileQuery q


-- | lift value to Expr
val :: (ToField a ) => a -> Expr a
val = term . rawField

infix 4 ==., <., <=., >., >=.

(==.) :: Expr a -> Expr a -> Expr Bool
a ==. b = binOp 15 (plain "=") a b

(>.) :: Expr a -> Expr a -> Expr Bool
a >. b = binOp 15 (plain ">") a b

(>=.) :: Expr a -> Expr a -> Expr Bool
a >=. b = binOp 15 (plain ">=") a b

(<.) :: Expr a -> Expr a -> Expr Bool
a <. b = binOp 15 (plain "<") a b

(<=.) :: Expr a -> Expr a ->  Expr Bool
a <=. b = binOp 15 (plain "<=") a b

infixr 3 &&.
(&&.), (||.) :: Expr Bool -> Expr Bool -> Expr Bool
a &&. b = binOp 18 (plain " AND ") a b

infixr 2 ||.
a ||. b = binOp 19 (plain " OR ") a b

-- | Access field of relation
not_ :: Expr Bool -> Expr Bool
not_ r = prefOp 17 (raw " NOT ") r

mkAcc :: KnownSymbol t => Rel r -> Field v t a -> Expr b
mkAcc rel fld = Expr 2 $ mkAccess rel (raw $ fieldColumn fld)

infixl 9 ~>, ?>
(~>) :: (KnownSymbol t) => Rel a -> Field a t b -> Expr b
(~>) = mkAcc

(?>) :: (KnownSymbol t) => Rel (Maybe a) -> Field a t b -> Expr (Maybe b)
(?>) = mkAcc

-- | like operator
(~.) :: Expr Text -> Expr Text -> Expr Bool
(~.) a b = binOp 14 (plain " ~ ") a b

isInList :: ToField a => (Expr a) -> [a] -> Expr Bool
isInList _ [] = false
isInList a l = binOp 11 (plain " IN ") a (term (lst))
  where
    lst = addParens $ D.fromList . intersperse (plain ",") $ map toField l

-- | Wrap in Only
only :: Expr a -> Expr (Only a)
only (Expr p a) = Expr p a

unonly :: Expr (Only a) -> Expr a
unonly (Expr p a) = Expr p a

true :: Expr Bool
true = term $ raw "true"

false :: Expr Bool
false = term $ raw "true"

just :: Expr a -> Expr (Maybe a)
just (Expr p r) = Expr p r

isNull :: Expr a -> Expr Bool
isNull r = binOp 7 (plain " IS ") r (term $ raw "NULL")

unsafeCast :: Expr a -> Expr b
unsafeCast (Expr p r) = Expr p r


array :: Expr a -> Expr (Vector a)
array (Expr _ r) = term $ raw "ARRAY["<> r <> raw "]"

array_append :: Expr (Vector a) -> Expr a -> Expr (Vector a)
array_append (Expr _ arr) (Expr _ v) = Expr 0 $ raw "array_append("
             <> arr <> raw "," <> v <> raw ")"

array_prepend :: Expr a -> Expr (Vector a) -> Expr (Vector a)
array_prepend (Expr _ v) (Expr _ arr) = Expr 0 $
  raw "array_prepend(" <> v <> raw "," <> arr <> raw ")"
array_cat :: Expr (Vector a) -> Expr (Vector a) -> Expr (Vector a)
array_cat (Expr _ a) (Expr _ b) = Expr 0 $
  raw "array_cat(" <> a <> raw "," <> b <> raw ")"

infixr 7 =., =.!
(=.) :: forall v t a. (KnownSymbol t) => Field v t a -> Expr a -> UpdExpr v
f =. (Expr _ a) = UpdExpr [(fieldColumn f, a)]

(=.!) :: (KnownSymbol t, ToField a) => Field v t a -> a -> UpdExpr v
(=.!) f e = f =. val e

set :: KnownSymbol t => Field v t a -> Expr a -> UpdExpr v
set f a = f =. a

setField :: (KnownSymbol t) => Field v t a -> Expr a -> Updating v ()
setField f a = setFields (f =. a)

setFields :: UpdExpr t -> Updating t ()
setFields (UpdExpr x) = Updating $ do
  st <- get
  put st { queryAction = queryAction st <> x }


updateTable :: forall a b. Table a => (Rel a -> Updating a b) -> Update b
updateTable f = update (table $ tableName (Proxy :: Proxy a)) f

update :: From (Rel a) -> (Rel a -> Updating a b) -> Update b
update r f = Update $ do
  ns <- grabNS
  let (FromTable nm alias expr, ns') = runState (runFrom r) ns
  modifyNS $ const ns'
  compileUpdating (Plain $ B.fromByteString nm <> B.fromByteString " AS " <> alias) $ f expr

insert :: From (Rel a) -> Query (UpdExpr a) -> Update (Rel a)
insert r mq = do
  ns <- grabNS
  let (FromTable nm _ _, ns') = runState (runFrom r) ns
      rel = Rel $ B.fromByteString nm
  modifyNS $ const ns'
  (UpdExpr upd, q) <- compIt mq
  compileInserting (plain nm) $ q {queryAction = upd}
  return rel

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
delete r f = do
  ns <- grabNS
  let (FromTable nm alias expr, ns') = runState (runFrom r) ns
  modifyNS $ const ns'
  compileDeleting (Plain $ B.fromByteString nm <> B.fromByteString " AS " <> alias) $ f expr

-- | Same as above but table is taken from table class
-- > deleteFromTable $ \u -> where & u~>UserKey ==. val uid
deleteFromTable :: forall a b .(Table a) => (Rel a -> Deleting a b) -> Update b
deleteFromTable f = delete tn f
  where
    tn = table $ tableName (Proxy :: Proxy a)

queryUpdate :: forall t b .(FromRecord t b, IsRecord t)
            => Connection -> Update t -> IO [b]
queryUpdate con u = do
    q <- buildQuery con body
    PG.queryWith_ parser con (PG.Query q)
  where
    (body, parser) = finishUpdate u
formatUpdate :: IsRecord t => Connection -> Update t -> IO ByteString
formatUpdate con u = buildQuery con (compileUpdate u)

executeUpdate :: Connection -> Update a -> IO Int64
executeUpdate con u = PG.execute_ con . PG.Query =<< buildQuery con (finishUpdateNoRet u)

withRecursive :: (IsRecord a) =>  Recursive a -> (From a -> Query b) -> Query b
withRecursive (Recursive un q f) ff = compileUnion un q f >>= ff

union :: Query a -> (From a -> Query a) -> Recursive a
union = Recursive UnionDistinct

unionAll :: Query a -> (From a -> Query a) -> Recursive a
unionAll = Recursive UnionAll

compileUnion :: (IsRecord a) => Union -> Query a -> (From a -> Query a) -> Query (From a)
compileUnion un q f = do
   nm <- newName
   (exprNonRec, queryNonRec) <- compIt q
   renamed <- asRenamed exprNonRec
   let frm = From . return $ FromQuery (exprToSeq nm) renamed
   (exprRec, queryRec) <- compIt $ f frm
   let bld = exprToSeq (namedRow nm renamed)
           <> (plain " AS (" <| finishIt (asValues exprNonRec) queryNonRec)
           <> ((unioner <| finishIt (asValues exprRec) queryRec) |> plain ")")
   modifyRecursive $ const True
   appendWith bld
   return frm
   where
     unioner = case un of
         UnionDistinct -> plain " UNION "
         UnionAll      -> plain " UNION ALL "
{-# INLINE compileUnion #-}

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
    appendFrom $ plain "(" <| compiled <> exprToSeq (raw ") AS " <> namedRow nm renamed)
    return renamed

groupBy :: Expr a -> ExprA a
groupBy (Expr _ a) = ExprGrp a

sum_ :: Num a => Expr a -> ExprA Int64
sum_ (Expr _ a) = ExprAgg (raw "sum(" <> a <> raw ")")

countAll :: ExprA Int64
countAll = ExprAgg (raw "count(*)")

count :: Expr a -> ExprA Int64
count (Expr _ a) = ExprAgg $ raw "count(" <> a <> raw ")"


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

buildQuery :: Connection -> Seq Action -> IO ByteString
buildQuery conn q = B.toByteString <$> foldlM sub mempty q
  where
    sub !acc (Plain b)       = pure $ acc <> b
    sub !acc (Escape s)      = (mappend acc . quote) <$> escapeStringConn conn s
    sub !acc (EscapeByteA s) = (mappend acc . quote) <$> escapeByteaConn conn s
    sub !acc (EscapeIdentifier s) = (mappend acc . B.fromByteString) <$> escapeIdentifier conn s
    sub !acc (Many xs)       = foldlM sub acc xs
    quote = inQuotes . B.fromByteString

cross :: (IsRecord a, IsRecord b) => From a -> From b -> From (a:.b)
cross = crossJoin

on :: (( a-> b-> Expr Bool) -> From (a:.b)) -> (a -> b -> Expr Bool) -> From (a:.b)
on f b = f b

inner :: forall a b .(IsRecord a, IsRecord b) =>
   From a -> From b -> (a -> b -> Expr Bool) -> From (a:.b)
inner = innerJoin

infixr 3 `inner`
infixl 2 `on`

infixr 3 `cross`

from' :: (IsQuery m) => From a -> (a -> m b) -> m b
from' (From a) f = do
  ns <- grabNS
  let (frm, ns') = runState a ns
      (cmp, expr) = compileFrom frm
  appendFrom cmp
  modifyNS $ const ns'
  r <- f expr
  return r

with' :: (IsRecord a, IsQuery m) => Query a -> (From a -> m b) -> m b
with' mq act = do
  (e, q) <- compIt mq
  renamed <- asRenamed e
  nm <- newName
  let bld = exprToSeq (namedRow nm renamed)
            <> (plain " AS ( " <| (finishIt (asValues e) q |> plain ")"))
  appendWith bld
  act $ From . return $ FromQuery (exprToSeq nm) renamed


exists' :: IsRecord a => Query a -> Expr a
exists' mq = term $ D.fromList . toList $ (plain " EXISTS (" <| body) |> plain ")"
  where
    body = compileQuery mq


rtable :: forall a . (IsRecord a) => Text -> From a
rtable nm = From $ do
  let bs = T.encodeUtf8 nm
      nameBld = B.fromByteString bs
  alias <- newName --grabAlias nameBld
  ren <- asRenamed (undefined :: a)
  let q = (pure $ Plain nameBld) <> raw " AS " <> alias <> addParens (commaSep (asValues ren))

  return $ FromQuery (exprToSeq q) ren

