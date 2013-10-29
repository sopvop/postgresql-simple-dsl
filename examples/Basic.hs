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


users = table "users" :: From (Rel User)
roles = table "project_roles" :: From (Rel Role)

-- | all roles for login
{-
get_class_path :: Expr UserId -> Expr String
get_class_path e = call . arg e $ function "get_class_path"
-}
printQ con = putStrLn . B.unpack <=< formatQuery con
main = do
  con <- connectPostgreSQL "dbname=testdb user=lonokhov password=batman"
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
  {-
  usrs <- query con (userRoles "oleg" & select)
  mapM_ print usrs
  let q = userRoles2 & where_ (\(u:._)->u~>UserId' ==. val (UserId 2))
  mapM_ print =<< query con (select q)
  print =<< formatQuery con (select q)
  let qq = userRoles "oleg" & select
                            & orderOn (\(name, _) -> descendOn name)
  print =<< formatQuery con qq
  print =<< query con qq
  let uq = update (\u-> u~>UserLogin ==. val "admin")
                    (UserCerebroLogin =. val "22")
                   & returning (\u -> (u~>UserId', u~>UserCerebroLogin))
  print =<< formatUpdate con uq
  print =<< executeUpdate con uq

  let ins = insert (UserLogin =. val "foo")
  print =<< formatUpdate con ins
  let ins2 = insertFrom (fromTable & where_ (\u->u~>UserLogin ==. val "admin"))
             (\u -> UserLogin =. val "foo" <> UserPass =. u~>UserPass)
  print =<< formatUpdate con ins2
  let del1 = delete (\u-> u~>UserLogin ==. val "foo")
  print =<< formatUpdate con del1
  let del2 = deleteFrom (fromTable & where_ (\u->u~>UserLogin ==. val "admin"))
            (\(ut:.u) -> u~>UserLogin ==. val "foo" &&. u~>UserPass ==. ut~>UserPass)
  print =<< formatUpdate con del2
  print =<< executeUpdate con del2
  let with1 = with (select $ fromTable & where_ (\u->u~>UserLogin ==. val "oleg"))
        $ \u1 -> with (select $ fromTable & where_ (\u->u~>UserLogin ==. val "ozaycev"))
         $ \u2 -> fromTable & where_
           (\r->r~>RoleUserId ==. u1~>UserId' ||. r~>RoleUserId ==. u2~>UserId')
      with2 = with1 & project (\r -> (r~>RoleName, get_class_path(r~>RoleUserId)))
  print =<< formatQuery con (select with2)
  mapM_ (print )  <=< query con $ select with2
  -}
