{-# LANGUAGE QuasiQuotes, OverloadedStrings #-}

import PostgreSQL
import Preface

main = do
    (cs : _) <- getArgs
 
    conn <- connectToDb (asText cs)

    let get1 x = doQuery conn (Query x)

    let schemaList = [qqstr|
SELECT n.nspname AS "Name"
-- ,pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;
|]

--    let searchPath = "set search_path="++ (intercalate "," ra)
    sl <- get1 schemaList
    print sl




