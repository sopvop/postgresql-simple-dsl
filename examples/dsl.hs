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
import Database.PostgreSQL.Simple           (Connection)
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
  UserPass  :: Field User "password" ByteString

instance Table User where
  tableName _ = "users"

instance Selectable User where
  entityParser _ = User <$> fromField UserId'
                        <*> fromField UserLogin
                        <*> fromField UserPass
instance FromRow User where
  fromRow = entityRowParser $ entityParser (undefined :: Proxy User)

data instance Field Role t a where
  RoleUserId :: Field Role "user_id" UserId
  RoleName   :: Field Role "name" ByteString

instance Table Role where
  tableName _ = "roles"

instance Selectable Role where
  entityParser _ = Role <$> fromField RoleUserId
                        <*> fromField RoleName

instance FromRow Role where
  fromRow = entityRowParser $ entityParser (undefined :: Proxy Role)

getAllUsers :: Connection -> IO [User]
getAllUsers c = select c $ fromTable

allUsers :: Query (Expr (Whole User))
allUsers = fromTable

allRoles :: Query (Expr (Whole Role))
allRoles = fromTable

-- | all roles for login
userRoles :: String -> Query (Expr (Only ByteString))
userRoles login =
  innerJoin (\(usr :. rol) -> usr ~> UserId' ==. rol ~> RoleUserId)
            (allUsers & where_ (\u -> u~>UserLogin ==. val login))
            allRoles
  & project (\(usr :. rol) -> only $ rol~>RoleName)
