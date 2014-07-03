{-# LANGUAGE OverloadedStrings #-}

import PostgreSQL as PG
import Control.Monad (forever)
import System.Environment (getArgs)
import qualified Data.ByteString as B (pack)
import qualified Data.Text as T (pack, unpack, singleton)
import qualified Data.Text.Encoding as T (encodeUtf8, decodeUtf8)

import Control.Concurrent (threadDelay)

main :: IO ()
main = do
    args <- getArgs
    
    conn <- PG.connectToDb "host=localhost port=5432 user=r0ml dbname=r0ml"
    
{-    
    d <- doQuery a (FunctionCall 1598 [])
    print d
-}    
    e <- sendQuery conn (Parse "stm" "select * from pg_tables where schemaname = $1" [1043])
    print e
    
    f <- sendQuery conn (Bind "clem" "stm" [Just $ (T.encodeUtf8 . T.pack) (head args)])
    print f
    
    g <- doQuery conn (Execute "clem" 3)
    print g
    
    print =<< sendQuery conn (ClosePortal "clem")
    print =<< sendQuery conn (CloseStatement "stm")

    print =<< doQuery conn Sync 


--    threadDelay 10000000

 {-   forever $ do
      h <- readResponse conn
      print h
   -}   
--------------------------------------------------------------------------------