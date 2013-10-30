{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}


module Database.PostgreSQL.Simple.Dsl.Entity
    ( getEntity
    , updateEntity
    , insertEntity
    , deleteEntity
    , Entity(..)
    ) where

import           Control.Monad                        (void)

import           Database.PostgreSQL.Simple           (Connection)
import           Database.PostgreSQL.Simple.Dsl
import           Database.PostgreSQL.Simple.FromField (FromField)
import           Database.PostgreSQL.Simple.ToField   (ToField)

class (Record a, Table a, ToField (EntityId a), FromField (EntityId a)) => Entity a where
   type EntityId a :: *
   idField :: Rel a -> Expr (EntityId a)
   entityUpdate :: a -> UpdExpr a


takeOne :: Monad m => [Whole a] -> m (Maybe a)
takeOne [] = return Nothing
takeOne (x:_) = return (Just $ getWhole x)
{-
takeOneE :: Monad m => [a] -> m a
takeOneE (x:_) = return x
takeOneE _ = error "takeOneE: Need proper error here"
-}
takeOnlyE :: Monad m => [Only a] -> m a
takeOnlyE (Only x:_) = return x
takeOnlyE _ = error "takeOnlyE: Need proper error here"

getEntity :: Entity a => Connection -> EntityId a -> IO (Maybe a)
getEntity c eid = takeOne =<< query c sel
   where
     sel = do
       t <- fromTable
       where_ (idField t ==. val eid)
       return t

updateEntity :: Entity a => Connection -> EntityId a -> UpdExpr a -> IO ()
updateEntity c eid upds = void $ executeUpdate c upd
   where
     upd = updateTable $ \t -> do
          setFields upds
          where_ $ idField t ==. val eid

insertEntity :: Entity a => Connection -> a -> IO (EntityId a)
insertEntity c ent = takeOnlyE =<< queryUpdate c ins
   where
    ins = insertIntoTable $ \r -> do
        setFields (entityUpdate ent)
        return $ only $ idField r

deleteEntity :: forall a . Entity a => Connection -> EntityId a -> IO ()
deleteEntity c eid = void $ executeUpdate c del
  where
    del = deleteFromTable $ \(r :: Rel a) -> do
       where_ $ idField r ==. val eid
