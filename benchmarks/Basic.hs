{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeFamilies          #-}

import           Control.Applicative

import qualified Data.ByteString.Char8          as B

import           Criterion
import           Criterion.Main                 (defaultMain)

import           Database.PostgreSQL.Simple     (Connection, connectPostgreSQL)
import           Database.PostgreSQL.Simple.Dsl


data Foo = Foo { fooInt :: Int, fooString :: String
               } deriving (Show)

instance Record Foo where
  data Field Foo t a where
    FooInt :: Field Foo "foo_int" Int
    FooString :: Field Foo "foo_string" String

  recordParser _ = Foo <$> takeField FooInt
                       <*> takeField FooString

foos :: From (Rel Foo)
foos = table "foo_table"

bench1 c l s i = formatQuery c $ do
  f <- from foos
  fb <- from foos
  where_ $ f~>FooInt `isInList` l &&. f~>FooString ==. val s
            ||. f~>FooInt >. val i &&. f~>FooInt ==. fb~>FooInt
  return (f :. fb)

{-# NOINLINE bench1 #-}

main :: IO ()
main = do
  con <- connectPostgreSQL "dbname=testdb"
  defaultMain [bench "simple" $ nfIO $ bench1 con [1..10] "foo" 20]
  B.putStrLn =<< bench1 con [1..10] "foo" 20
