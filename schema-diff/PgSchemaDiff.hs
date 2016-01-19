{-# LANGUAGE FlexibleInstances, QuasiQuotes, OverloadedStrings #-}

import PostgreSQL
import Preface

-- import Acl
import Proc
import View
import Table
-- import XferData
-- import UDT
import Trigger

schemaList = [str|
SELECT n.nspname AS "Name"
 -- ,pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;
|]

initialize :: [Text] -> IO (Text -> IO PgResult, Text -> IO PgResult) 
initialize args = do
    let (conns1 : conns2 : restArgs ) = args

    conn1 <- connectToDb conns1
    conn2 <- connectToDb conns2

    let get1 x = doQuery conn1 (Query x)
    let get2 x = doQuery conn2 (Query x)
    ra <- if null restArgs then do
                sn1 <- get1 schemaList
                let (ResultSet fd ds _) = sn1
                return $ map (stringField . head) ds
          else return restArgs

    putStrLn ("Schema list = "++ show ra)
    let searchPath = strConcat ["set search_path=", intercalate "," ra]
    get1 searchPath >>= print
    get2 searchPath >>= print
    return (get1, get2)

{- The command line arguments are:
   1) a comma separated list of which things to diff 
   2) the first connection string
   3) the second connection string
   4) the list of schemas to compare
-}

main :: IO ()
main = do
  args <- getArgs
  let which = head args
      ag = map asText (tail args)

  case which of
     "procs" -> initialize ag >>= compareProcs >>= mapM_ print
     "views" -> initialize ag >>= compareViews >>= mapM_ print
     "triggers" -> initialize ag >>= compareTriggers >>= mapM_ print
     -- "xfer" -> initialize ag >>= xferData >>= mapM print
     "tables" -> initialize ag >>= compareTables >>= mapM_ print
     -- "types" -> initialize ag >>= compareTypes >>= mapM print
     _ -> mapM_ putStr [ [str|
The valid comparisons are: procs, views, triggers, tables, types

The arguments are:
  comparisonType orangeConnectionString blueConnectionString schema1 schema2

The connectionStrings are of the form:
  "host={hostname} port={portNumber} user={userid} dbname={dbname}"
|] ]


  -- disconnect conn1
  -- disconnect conn2

