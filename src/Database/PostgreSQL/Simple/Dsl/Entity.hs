{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}


module Database.PostgreSQL.Simple.Dsl.Entity
    ({- getEntity
    , updateEntity
    , insertEntity
    , deleteEntity
    , Entity(..)
    , Storable(..) -}
    ) where

import           Control.Monad                           (void)

import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          Only (..))
import           Database.PostgreSQL.Simple.Dsl
import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.FromField    (FromField)
import           Database.PostgreSQL.Simple.ToField      (Action (..),
                                                          ToField (..))
{-
class (Selectable a, Table a, ToField (EntityId a), FromField (EntityId a)) => Entity a where
   type EntityId a :: *
   idField :: Expr (Whole a) -> Expr (EntityId a)

class (Entity a) => Storable a where
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
     sel = select . where_ (\en ->idField en ==. val eid) $ fromTable

updateEntity :: Entity a => Connection -> EntityId a -> UpdExpr a -> IO ()
updateEntity c eid upds = void $ executeUpdate c upd
   where
     upd = update (\u -> idField u ==. val eid) upds

insertEntity :: Storable a => Connection -> a -> IO (EntityId a)
insertEntity c ent = takeOnlyE =<< queryUpdate c ins
   where
    ins = returning (\e -> only (idField e)) $ insert (entityUpdate ent)

deleteEntity :: forall a . Entity a => Connection -> EntityId a -> IO ()
deleteEntity c eid = void $ executeUpdate c del
  where
    del = delete (\(u::Expr (Whole a)) -> idField u ==. val eid)
-}
