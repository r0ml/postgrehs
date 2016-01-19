{-# LANGUAGE OverloadedStrings #-}

import PostgreSQL as PG
import Preface

main :: IO ()
main = do
    (a1:a2:_) <- getArgs
    
    conn <- PG.connectToDb (asText a1)
    
{-    
    d <- doQuery a (FunctionCall 1598 [])
    print d
-}    
    e <- sendQuery conn (Parse "stm" "select * from pg_tables where schemaname = $1" [1043])
    print e
    
    f <- sendQuery conn (Bind "clem" "stm" [Just $ asByteString a2])
    print f
    
    g <- doQuery conn (Execute "clem" 0) -- number of rows to return
    print g
    
    print =<< sendQuery conn (ClosePortal "clem")
    print =<< sendQuery conn (CloseStatement "stm")

    print =<< doQuery conn Sync 

    -- print "disconnect?"

    -- disconnect conn
    
    -- print "done"

--    threadDelay 10000000

 {-   forever $ do
      h <- readResponse conn
      print h
   -}   
--------------------------------------------------------------------------------
