{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE GADTs                      #-}

import Data.ByteString (ByteString)

import Control.Applicative

import Data.Monoid
import Data.Proxy
import Database.PostgreSQL.Simple           (Connection, connectPostgreSQL)
import Database.PostgreSQL.Simple.Dsl
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
                 , userCerebro :: String
                 , userPassword :: ByteString
                 } deriving (Show)

data Role = Role { roleUserId :: UserId, roleName :: ByteString }
        deriving (Show)

data instance Field User t a where
  UserId'   :: Field User "id" UserId
  UserLogin :: Field User "login" String
  UserCerebroLogin :: Field User "cerebro_login" String
  UserPass  :: Field User "passwd" ByteString

instance Table User where
  tableName _ = "users"

instance Selectable User where
  entityParser _ = User <$> fromField UserId'
                        <*> fromField UserLogin
                        <*> fromField UserCerebroLogin
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
  con <- connectPostgreSQL "dbname=testdb user=lonokhov password=batman"
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
  let uq = update (\u-> u~>UserLogin ==. val "admin")
                    (UserCerebroLogin =. val "22")
                   & returningU (\u -> (u~>UserId', u~>UserCerebroLogin))
  print =<< executeUpdate con uq
  print =<< formatUpdate con uq
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
