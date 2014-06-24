{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE OverlappingInstances  #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ConstraintKinds       #-}

import           Control.Applicative

import qualified Data.ByteString.Char8 as B

import           Criterion
import           Criterion.Main                 (defaultMain)

import           Database.PostgreSQL.Simple     (Connection, connectPostgreSQL)
import qualified Database.PostgreSQL.Simple     as PG
import           Database.PostgreSQL.Simple.Dsl
import           Database.PostgreSQL.Simple.Dsl.Record


data Foo = Foo { fooInt :: Int, fooString :: String
               } deriving (Show)

instance Record Foo where
  data Field Foo t a where
    FooInt :: Field Foo "foo_int" Int
    FooString :: Field Foo "foo_string" String

instance FromRecord (Rel Foo) Foo where
  fromRecord r = Foo <$> recField (r~>FooInt)
                     <*> recField (r~>FooString)

foos :: From (Rel Foo)
foos = table "foo_table"

bench1 c l s i = formatQuery c $ do
  f <- from foos
  fb <- from foos
  let fint = f~>FooInt
      fstring = f~>FooString
  where_ $ fint `isInList` l &&. fstring ==. val s
            ||. fint >. val i &&. fint ==. fb~>FooInt
  return (f :. fb)

{-# NOINLINE bench1 #-}

foo_string = SField :: "foo_string" ::: Expr String
foo_int = SField :: "foo_int" ::: Expr Int

foo_table :: From (Rec '["foo_string" ::: Expr String, "foo_int" ::: Expr Int])
foo_table = rtable "foo_table"

bench2 c l s i = formatQuery c $ do
  f <- from foo_table
  fb <- from foo_table
  let fint = rGet foo_int f
      fstring = rGet foo_string f
      
  where_ $ fint `isInList` l &&. fstring ==. val s
            ||. fint >. val i &&. fint ==. rGet foo_int fb
  return (f :. fb)

{-# NOINLINE bench2 #-}

bench3 :: Connection -> [Int] -> String -> Int -> IO B.ByteString
bench3 c l s i = PG.formatQuery c q (PG.In l, s,  i)
  where
    q = "select q.foo_string, q.foo_int from foo_table \
        \ where q.foo_int IN ? AND q.foo_string = ? \
        \     or q.foo_int = ?"

{-# NOINLINE bench3 #-}        
main :: IO ()
main = do
  con <- connectPostgreSQL "dbname=testdb"
  defaultMain [bench "simple" $ nfIO $ bench1 con [1..10] "foo" 20
              , bench "record" $ nfIO $ bench2 con [1..10] "foo" 20
              , bench "plain" $ nfIO $ bench3 con [1..10] "foo" 20
              ]
  B.putStrLn =<< bench1 con [1..10] "foo" 20
  B.putStrLn =<< bench2 con [1..10] "foo" 20