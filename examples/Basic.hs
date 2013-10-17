{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE GADTs                      #-}

import Data.ByteString (ByteString)

import Control.Applicative

import Data.Proxy
import Database.PostgreSQL.Simple.Dsl
import Database.PostgreSQL.Simple.Dsl.Entity
import Database.PostgreSQL.Simple           (Connection, connectPostgreSQL)
import Database.PostgreSQL.Simple.FromField hiding (Field, fromField)
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
                 , userPassword :: ByteString
                 } deriving (Show)

data Role = Role { roleUserId :: UserId, roleName :: ByteString }
        deriving (Show)

data instance Field User t a where
  UserId'   :: Field User "id" UserId
  UserLogin :: Field User "login" String
  UserPass  :: Field User "passwd" ByteString

instance Table User where
  tableName _ = "users"

instance Selectable User where
  entityParser _ = User <$> fromField UserId'
                        <*> fromField UserLogin
                        <*> fromField UserPass

data instance Field Role t a where
  RoleUserId :: Field Role "user_id" UserId
  RoleName   :: Field Role "role" ByteString

instance Table Role where
  tableName _ = "project_roles"

instance Selectable Role where
  entityParser _ = Role <$> fromField RoleUserId
                        <*> fromField RoleName

instance FromRow Role where
  fromRow = entityRowParser $ entityParser (undefined :: Proxy Role)

getAllUsers :: Connection -> IO [Whole User]
getAllUsers c = select c $ fromTable & finish

allUsers :: Select (Whole User)
allUsers = fromTable

allRoles :: Select (Whole Role)
allRoles = fromTable

-- | all roles for login
userRoles :: String -> Select (ByteString, UserId)
userRoles login =
  innerJoin (\(usr :. rol) -> usr ~> UserId' ==. rol ~> RoleUserId)
            (allUsers & where_ (\u -> u~>UserLogin ==. val login))
            allRoles
  & project (\(usr :. rol) -> (rol~>RoleName, rol~>RoleUserId))

userRoles2 = crossJoin fromTable fromTable
           & where_ (\(u:.rol) -> u~>UserId' ==. rol~>RoleUserId)

main = do
  con <- connectPostgreSQL "dbname=testdb user=test password=batman"
  formatQuery con (userRoles "foo" & finish)
  usrs <- select con (userRoles "oleg" & finish)
  mapM_ print usrs
  let q = userRoles2 & where_ (\(u:._)->u~>UserId' ==. val (UserId 2))
  mapM_ print =<< select con (finish q)
  print =<< formatQuery con (finish q)
  let qq = userRoles "oleg" & finish
                            & orderOn (\(name, _) -> descendOn name)
  print =<< formatQuery con qq
  print =<< select con qq

