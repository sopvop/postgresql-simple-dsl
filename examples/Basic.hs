{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE GADTs                      #-}

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B

import Control.Applicative
import Control.Monad

import Data.Monoid
import Data.Proxy
import Database.PostgreSQL.Simple           (Connection, connectPostgreSQL)
import Database.PostgreSQL.Simple.Dsl
import Database.PostgreSQL.Simple.FromField hiding (Field, takeField)
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField

infixl 1 &

(&) :: a -> (a -> b) -> b
a & f = f a
{-# INLINE (&) #-}


newtype UserId = UserId { getUserId :: Int}
        deriving (Show, ToField, FromField, Eq, Ord)

data User = User { userId :: UserId
                 , userLogin :: String
                 , userCerebro :: String
                 , userPassword :: ByteString
                 } deriving (Show)

data Role = Role { roleUserId :: UserId, roleName :: ByteString }
        deriving (Show)

instance Table User where
  tableName _ = "users"

instance Record User where
  data Field User t a where
    UserKey   :: Field User "id" UserId
    UserLogin :: Field User "login" String
    UserCerebroLogin :: Field User "cerebro_login" String
    UserPass  :: Field User "passwd" ByteString
  recordParser _ = User <$> takeField UserKey
                        <*> takeField UserLogin
                        <*> takeField UserCerebroLogin
                        <*> takeField UserPass

instance Table Role where
  tableName _ = "project_roles"

instance Record Role where
  data Field Role t a where
    RoleUserId :: Field Role "user_id" UserId
    RoleName   :: Field Role "role" ByteString

  recordParser _ = Role <$> takeField RoleUserId
                        <*> takeField RoleName

instance FromRow Role where
  fromRow = recordRowParser $ recordParser (undefined :: Proxy Role)

getAllUsers :: Connection -> IO [Whole User]
getAllUsers c = query c $ fromTable

data AssetClass = AssetClass
  { classId :: Int
  , classParent :: Maybe Int
  } deriving (Show)

instance Record AssetClass where
  data Field AssetClass t a where
    ClassKey :: Field AssetClass "id" Int
    ClassParent :: Field AssetClass "parent_id" (Maybe Int)
  recordParser _ = AssetClass <$> takeField ClassKey <*> takeField ClassParent

classes = table "asset_class" :: From (Rel AssetClass)

users = table "users" :: From (Rel User)
roles = table "project_roles" :: From (Rel Role)

get_class_path :: Expr UserId -> Expr String
get_class_path e = call . arg e $ function "get_class_path"

printQ con = putStrLn . B.unpack <=< formatQuery con
printU con = putStrLn . B.unpack <=< formatUpdate con
main = do
  con <- connectPostgreSQL "dbname=testdb2 user=lonokhov password=batman"
  return ()
  let qU = do
         x <- from users
         y <- fromTable
         where_ $ x~>UserLogin ==. val "admin" &&. y~>UserLogin ==. x~>UserLogin
         return (x:.y)
  printQ con qU
  query con qU
  let adminQ = do u <- from users
                  where_ $ u~>UserLogin ==. val "oleg"
                  return u
  let qUr = with adminQ $ \usr -> do
                 r <- from roles
                 where_ $ usr~>UserKey ==. r~>RoleUserId
                 return r
  printQ con qUr
  query con qUr
  let qUrr = do x <- subSelect $ do
                       u <- from users
                       where_ (u~>UserLogin ==. val "oleg")
                       return u
                r <- from roles
                where_ $ r~>RoleUserId ==. x~>UserKey
                orderBy $ ascendOn (r~>RoleName) <> descendOn (x~>UserLogin)
                limitTo 1
                return (r :. only (x~>UserKey))
  printQ con qUrr
  query con qUrr
  let updQ = update users $ \u -> do
               setField UserCerebroLogin (val "baz")
               where_ $ u~>UserLogin ==. val ("admin")
               return u
  printU con updQ
  queryUpdate con updQ
  mapM_ (putStrLn . fromOnly) <=< query con $ do
     x <- from users
     where_ $ x~>UserLogin ==. val "admin"
     return $ only (x~>UserCerebroLogin)
  let nonrec = do
        c <- from classes
        where_ $ c~>ClassKey ==. val 788
        return c
  let recur rc = do
        c <- from classes
        where_ $ just (c~>ClassKey) ==. (rc~>ClassParent)
        return c
  printQ con $ withRecursive (nonrec `union` recur) $ return
  mapM_ (print . getWhole) <=< query con $ withRecursive (nonrec `union` recur) return
  let doIns = do
        (i, nm) <- fromPureValues [(UserId 1, "foo"), (UserId 2, "baz")]
        return $ RoleName =. nm <> RoleUserId =. i
  printU con $ insertIntoTable doIns
