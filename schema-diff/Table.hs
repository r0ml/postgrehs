{-# LANGUAGE QuasiQuotes, FlexibleInstances, OverloadedStrings #-}
{-# LANGUAGE DisambiguateRecordFields #-}

module Table where

import Acl
import Util

import PostgreSQL
import Preface

tblList :: String
tblList = [qqstr| 
SELECT n.nspname AS "Schema", c.relname AS "Name", d.description AS "Comment",
  relacl AS "ACLs"
FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid 
  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)
	-- LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')
	-- LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')
WHERE n.nspname IN (select * from unnest(current_schemas(false)))
  AND c.relkind = 'r'
ORDER BY 1, 2
|]

tblColumns :: String
tblColumns = [qqstr|
SELECT (c.oid, n.nspname,c.relname, c.relacl),
  a.attname, a.atttypid, a.atttypmod,a.attlen,
  a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
  row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
  pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,
  t.typbasetype,t.typtype
FROM pg_catalog.pg_namespace n 
JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
WHERE n.nspname IN (select * from unnest(current_schemas(false)))
  AND c.relkind='r'
  AND NOT a.attisdropped 
ORDER BY nspname, relname, attnum 
|]



tblIndices2 = [qqstr|
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE,   NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME,   CASE i.indisclustered     WHEN true THEN 1    ELSE CASE am.amname       WHEN 'hash' THEN 2      ELSE 3    END   END AS TYPE,   (i.keys).n AS ORDINAL_POSITION,   pg_catalog.pg_get_indexdef(ci.oid, (i.keys).n, false) AS COLUMN_NAME,   CASE am.amcanorder     WHEN true THEN CASE i.indoption[(i.keys).n - 1] & 1       WHEN 1 THEN 'D'       ELSE 'A'     END     ELSE NULL   END AS ASC_OR_DESC,   ci.reltuples AS CARDINALITY,   ci.relpages AS PAGES,   pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indoption,           i.indisunique, i.indisclustered,
i.indpred,           i.indexprs,           information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i     ON (ct.oid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)   JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) WHERE true  AND n.nspname = 'account' AND ct.relname = 'user_table' ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION
|]

tblIndices = [qqstr|
select ind.indisclustered, ind.indexrelid, ind.indisprimary, cls.relname  from pg_catalog.pg_index ind, pg_catalog.pg_class tab, pg_catalog.pg_namespace sch, pg_catalog.pg_class cls  where ind.indrelid = tab.oid  and cls.oid = ind.indexrelid  and tab.relnamespace = sch.oid  and tab.relname = $1 and sch.nspname = $2
|]

tblConstraints = [qqstr|
SELECT cons.conname, cons.conkey
FROM pg_catalog.pg_constraint cons, pg_catalog.pg_class tab, pg_catalog.pg_namespace sch 
WHERE cons.contype = 'u'  and cons.conrelid = tab.oid  and tab.relnamespace = sch.oid 
  AND tab.relname = $1 and sch.nspname = $2
|]

tblKeysx = [qqstr|
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (i.keys).n AS KEY_SEQ, ci.relname AS PK_NAME FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary,              information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i     ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = 'account' AND ct.relname = 'user_table' AND i.indisprimary  ORDER BY table_name, pk_name, key_seq
|]

tblKeys = [qqstr|
SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, pos.n AS KEY_SEQ, CASE con.confupdtype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS UPDATE_RULE, CASE con.confdeltype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS DELETE_RULE, con.conname AS FK_NAME, pkic.relname AS PK_NAME, CASE  WHEN con.condeferrable AND con.condeferred THEN 5 WHEN con.condeferrable THEN 6 ELSE 7 END AS DEFERRABILITY  FROM  pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka,  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka,
pg_catalog.pg_constraint con,  pg_catalog.generate_series(1, 32) pos(n),  pg_catalog.pg_depend dep, pg_catalog.pg_class pkic  WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid  AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid  AND con.contype = 'f' AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND pkic.relkind = 'i' AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid  AND fkn.nspname = 'account' AND fkc.relname = 'user_table' ORDER BY pkn.nspname,pkc.relname,pos.n
|]


{-
viewList = [str| 
SELECT n.nspname AS "Schema", c.relname AS "Name", -- d.description AS "Comment",
  pg_get_viewdef(c.oid) AS definition,
  relacl AS "ACLs"
FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid 
  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)
WHERE n.nspname IN (select * from unnest(current_schemas(false)))
  AND c.relkind = 'v'
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY 1, 2
|]

viewColumns = [qqstr|
SELECT n.nspname as "Schema",c.relname AS "View",a.attname AS "Column",a.atttypid AS "Type",
  a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
  a.atttypmod,a.attlen,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
  pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
  dsc.description,t.typbasetype,t.typtype
FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') 
WHERE a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]

viewTriggers = [str|
SELECT n.nspname as "Schema", c.relname AS "View", t.tgname AS "Name", t.tgenabled = 'O' AS enabled,
  -- pg_get_triggerdef(trig.oid) as source
  concat (np.nspname, '.', p.proname) AS procedure
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid  
JOIN pg_catalog.pg_proc p ON t.tgfoid = p.oid
JOIN pg_catalog.pg_namespace np ON p.pronamespace = np.oid
WHERE t.tgconstraint = 0  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]

viewRules = [str|
SELECT n.nspname as "Schema", c.relname AS "View", r.rulename AS "Name", pg_get_ruledef(r.oid) AS definition
FROM pg_rewrite r
JOIN pg_class c ON c.oid = r.ev_class
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname IN (select * from unnest(current_schemas(false))) AND c.relkind = 'v'
ORDER BY 1,2,3
|]
-}

data DbTable = DbTable { schema :: Text, name :: Text, comment :: Text, 
  columns :: [DbColumn], acl :: [Acl] } deriving(Show)

data DbColumn = DbColumn {
  attName :: Text,
  attTypeId :: Int,
  attNotNull :: Bool,
  attTypMod :: Int,
  attLen :: Int,
  attNum :: Int,
  adSrc :: Text,
  typBaseType :: Int,
  typType :: Bool
} deriving(Show)

instance PgValue DbTable where
  fromPg x = let [a,b,c,d] = fromPg x in DbTable a b c [] (cvtacl d)

instance PgValue DbColumn where
  fromPg = undefined

{-
{
  schema = fromPg s,
  name = fromPg n,
  comment = fromPg d,
  acl = cvtacl (fromPg acl)
}
-}

{-
amkdbt :: ([DbTable], [DbColumn]) -> [FieldValue] -> ([DbTable], [DbColumn])
amkdbt (tl, cl) nr = 
  let pr = mkdbt 
-}

instance Show (Comparison DbTable) where
    show (Equal x) = strConcat [ asString sok, asString $ showTable x, asString treset]
    show (LeftOnly a) = concat [asString azure, stringleton charLeftArrow," ", asString $ showTable a, asString treset]
    show (RightOnly a) = concat [asString peach, stringleton charRightArrow, " ", asString $ showTable a,  asString treset]
    show (Unequal a b) = concat [asString nok, asString $ showTable a,  asString treset, 
         showAclDiffs (acl a) (acl b)
         ]

instance Comparable DbTable where
  objCmp a b =
    if (acl a == acl b) then Equal a
    else Unequal a b

compareTables :: (Text -> IO PgResult, Text -> IO PgResult) -> IO [Comparison DbTable]
compareTables (get1, get2) = do
    aa <- get1 (asText tblColumns)
    let (ResultSet _ aa1 _) = aa

    -- aac <- get1 viewColumns
    -- aat <- get1 viewTriggers
    -- aar <- get1 viewRules

    bb <- get2 (asText tblColumns)
    let (ResultSet _ bb1 _) = bb
    -- bbc <- get2 viewColumns
    -- bbt <- get2 viewTriggers
    -- bbr <- get2 viewRules

    let a = map (fromPg . head)  aa1 :: [DbTable]
    let b = map (fromPg . head)  bb1 :: [DbTable]

    let cc = dbCompare a b
    let cnt = dcount iseq cc
    strPut $ if (fst cnt > 0) then asString sok ++ (show $ fst cnt) ++ " matches, " else ""
    strPutLn $ if (snd cnt > 0) then concat [asString $ setColor dullRed,show $ snd cnt," differences"] else concat [asString sok,"no differences"]
    strPut treset
    return $ filter (not . iseq) cc

showTable :: DbTable -> Text
showTable x = strConcat [schema x, ".", name x]

instance Ord DbTable where
  compare a b = let hd p = map ($ p) [schema, name] in compare (hd a) (hd b)

instance Eq DbTable where
  (==) a b = EQ == compare a b
