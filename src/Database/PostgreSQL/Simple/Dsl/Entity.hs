{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}


module Database.PostgreSQL.Simple.Dsl.Entity
    (
    {-
     Entity(..)
    , Storable(..)
    , Update
    , (=.)
    , setField
    , getEntity
    , updateEntity
    , updateEntityReturning
    , insertEntity
    , insertFieldsReturning
    -}
    ) where

import           Control.Monad                           (liftM, void)

import           Blaze.ByteString.Builder.ByteString     as B
import           Blaze.ByteString.Builder.Char8          as B
import           Data.ByteString                         (ByteString)
import           Data.List                               (intersperse)
import           Data.Monoid
import           Data.Proxy
import           GHC.TypeLits                            (SingI)

import           Database.PostgreSQL.Simple              ((:.) (..), Connection,
                                                          Only (..), execute,
                                                          query)
import           Database.PostgreSQL.Simple.Dsl.Internal
import           Database.PostgreSQL.Simple.FromField    (FromField)
import           Database.PostgreSQL.Simple.ToField      (Action (..),
                                                          ToField (..))

class (Selectable a, Table a, ToField (EntityId a), FromField (EntityId a)) => Entity a where
   type EntityId a :: *
   entityIdColumn :: Proxy a -> ByteString
   entityIdColumn _ = "id"

class (Entity a) => Storable a where
   entityUpdate :: a -> Update a

{-
takeOne :: Monad m => [Whole a] -> m (Maybe a)
takeOne [] = return Nothing
takeOne (x:_) = return (Just $ getWhole x)

takeOneE :: Monad m => [a] -> m a
takeOneE (x:_) = return x
takeOneE _ = error "takeOneE: Need proper error here"

makeColumnsAction :: [ByteString] -> Action
makeColumnsAction bs = Plain . mconcat $ intersperse (B.fromChar ',') cols
  where
     cols = map B.fromByteString bs

makeListOfActions :: [Action] -> Action
makeListOfActions = Many . intersperse (plain ",")

getIdAndTable :: Entity a => Proxy a -> (Action, Action)
getIdAndTable p = (idcol, table)
  where
   idcol = Plain . B.fromByteString $ entityIdColumn p
   table = Plain . B.fromByteString $ tableName p

getEntity :: forall a . (Entity a) => Connection -> EntityId a -> IO (Maybe a)
getEntity c eid = takeOne =<< query c "SELECT ? FROM ? WHERE ?=? LIMIT 1"
                            [columns, table, idcol, toField eid]
   where
     (idcol, table) = getIdAndTable (undefined :: Proxy a)
     columns = makeColumnsAction . entityColumns $ entityParser (undefined :: Proxy a)

-- | Update entity
updateEntity :: forall a . (Entity a) => Connection -> EntityId a -> Update a -> IO ()
updateEntity c eid (Update upds) =
    void $ execute c "UPDATE ? SET (?)=(?) WHERE ?=?"
            [table, columns, vals, idcol, toField eid]
   where
     columns = makeColumnsAction $ map fst upds
     vals = makeListOfActions $ map snd upds
     (idcol, table) = getIdAndTable (undefined :: Proxy a)

-- | Update and return whole entity
updateEntityReturning :: forall a . (Entity a) => Connection -> EntityId a -> Update a -> IO a
updateEntityReturning c eid (Update upds) =
   liftM getWhole $ takeOneE =<< query c "UPDATE ? SET (?)=(?) WHERE ?=? RETURNING ?"
            [table, columns, vals, idcol, toField eid, allColumns]
   where
     columns = makeColumnsAction $ map fst upds
     vals = makeListOfActions $ map snd upds
     (idcol, table) = getIdAndTable (undefined :: Proxy a)
     allColumns = makeColumnsAction . entityColumns $ entityParser (undefined :: Proxy a)

-- | insert new entity
insertEntity :: forall a . (Storable a) => Connection -> a -> IO (EntityId a)
insertEntity c v = liftM fromOnly $ takeOneE =<< query c "INSERT INTO ? (?) VALUES (?)"
             [table, columns, vals]
  where
     Update upds = entityUpdate v
     columns = makeColumnsAction $ map fst upds
     vals = makeListOfActions $ map snd upds
     (_, table) = getIdAndTable (undefined :: Proxy a)

-- | Insert partial fields returning whole entity
insertFieldsReturning :: forall a . (Entity a) => Connection -> Update a -> IO (EntityId a, a)
insertFieldsReturning c (Update upds) =
   liftM toResult $ takeOneE =<< query c "INSERT INTO ? (?) VALUES (?) RETURNING ?,?"
            [table, columns, vals, idcol, allColumns]
   where
     columns = makeColumnsAction $ map fst upds
     vals = makeListOfActions $ map snd upds
     (idcol, table) = getIdAndTable (undefined :: Proxy a)
     allColumns = makeColumnsAction . entityColumns $ entityParser (undefined :: Proxy a)
     toResult (Only a :. Whole b) = (a,b)
-}
