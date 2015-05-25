{-# LANGUAGE QuasiQuotes, FlexibleInstances, OverloadedStrings #-}

module View where

import Acl
import Util
import Diff

import PostgreSQL
import Preface.R0ml

import Data.Maybe

	-- LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')
	-- LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')
viewList :: String
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

viewColumns = [str|
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

data DbView = DbView { schema :: Text, name :: Text, definition :: Text, acl :: [Acl] }
  deriving(Show)

mkdbv :: [FieldValue] -> DbView
mkdbv (s : n : src : acl : _ ) = DbView {
  schema = stringField s,
  name = stringField n,
  definition = stringField src,
  acl = cvtacl (stringField acl)
}


instance Show (Comparison DbView) where
    show (Equal x) = concat [asString sok, asString $ showView x,  asString treset]
    show (LeftOnly a) = concat [asString azure, stringleton charLeftArrow," ", asString $ showView a, asString treset]
    show (RightOnly a) = concat [asString peach, stringleton charRightArrow, " ", asString $ showView a,  asString treset]
    show (Unequal a b) = concat [asString nok, asString $ showView a,  asString treset, 
         -- if (acl a /= acl b) then concat[ setAttr bold, "\n  acls: " , treset, map show $ dbCompare a b] else "",
         showAclDiffs (acl a) (acl b),
         if (compareIgnoringWhiteSpace (definition a) (definition b)) then ""
            else concat [asString $ setAttr bold,"\n  definition differences: \n", asString treset, concatMap show $ diff (asString (definition a)) (asString (definition b))]
         ]

instance Comparable DbView where
  objCmp a b =
    if (acl a == acl b && compareIgnoringWhiteSpace (definition a) (definition b)) then Equal a
    else Unequal a b

compareViews :: (Text -> IO PgResult, Text -> IO PgResult) -> IO [Comparison DbView]
compareViews (get1, get2) = do
    aa <- get1 (asText viewList)

    let (ResultSet _ aa1 _) = aa
    -- aac <- get1 viewColumns
    -- aat <- get1 viewTriggers
    -- aar <- get1 viewRules

    bb <- get2 (asText viewList)
    let (ResultSet _ bb1 _ ) = bb
    -- bbc <- get2 viewColumns
    -- bbt <- get2 viewTriggers
    -- bbr <- get2 viewRules

    let a = map mkdbv aa1
    let b = map mkdbv bb1

    let cc = dbCompare a b
    let cnt = dcount iseq cc
    putStr $ if (fst cnt > 0) then asString sok ++ (show $ fst cnt) ++ " matches, " else ""
    putStrLn $ if (snd cnt > 0) then concat [asString $ setColor dullRed,show $ snd cnt," differences"] else concat [asString sok,"no differences"]
    strPut treset
    return $ filter (not . iseq) cc

showView :: DbView -> Text
showView x = strConcat [schema x,  ".", name x]

instance Ord DbView where
  compare a b = let hd p = map ($ p) [schema, name] in compare (hd a) (hd b)

instance Eq DbView where
  (==) a b = EQ == compare a b
