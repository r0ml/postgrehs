{-# LANGUAGE QuasiQuotes, FlexibleInstances, OverloadedStrings #-}

module Proc where

import PostgreSQL
import Preface
import Str(str)
import Acl
import Util
import Diff

import qualified Data.ByteString as B
import Data.Maybe

functionList :: String
functionList = [str|
SELECT n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
 CASE
  WHEN p.proisagg THEN 'agg'
  WHEN p.proiswindow THEN 'window'
  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
  ELSE 'normal'
 END as "Type",
 p.prosrc as "Source",
 p.proacl::varchar as "ACL"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE pg_catalog.pg_function_is_visible(p.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
ORDER BY 1, 2, 3;
|]

data DbProc = DbProc { schema :: Text, name :: Text, argTypes :: Text, resType :: Text, ptype :: Text,
                       source :: Text, acl :: [Acl] } deriving(Show, Eq)

-- FieldValue is Maybe ByteString
mkdbp :: [FieldValue] -> DbProc
mkdbp (s : n : a : r : p : src : acl : _ ) = DbProc {
  schema = stringField s,
  name = stringField n,
  argTypes = stringField a,
  resType = stringField r,
  ptype = stringField p,
  source = stringField src,
  acl = cvtacl (stringField acl)
  }

-- mkdbp (a:b:c:d:e:f:g:_) = DbProc a b c d e f (cvtacl g)

showProc :: DbProc -> String
showProc x = asString (strConcat [schema x, ".", name x, "(", argTypes x, ")" ])

instance Show (Comparison DbProc) where
  show (Equal x) = concat [asString sok, showProc x,  asString treset]
  show (LeftOnly a) = concat [asString azure, stringleton charLeftArrow," ", showProc a, asString treset]
  show (RightOnly a) = concat [asString peach, stringleton charRightArrow, " ", showProc a,  asString treset]
  show (Unequal a b) = concat [asString nok, showProc a, asString  treset, 
       if (resType a /= resType b) then concat [asString $ setAttr bold,"\n  resultTypes: ", asString treset, asString $ resType a, asString neq , asString $ resType b] else "",
       -- if (acl a /= acl b) then concat[ setAttr bold, "\n  acls: " , treset, intercalate ", " $ acl a, neq,  intercalate ", " $ acl b] else "",
       if (compareIgnoringWhiteSpace (source a) (source b)) then ""
          else concat [asString $ setAttr bold,"\n  source differences: \n", asString treset, concatMap show $ diff (split '\n' $ source a) (split '\n' $  source b)]
       ]

instance Comparable DbProc where
  objCmp a b = 
    if (resType a == resType b && acl a == acl b && compareIgnoringWhiteSpace (source a) (source b)) then Equal a
    else Unequal a b

compareProcs :: (Text -> IO PgResult, Text -> IO PgResult) -> IO [Comparison DbProc]
compareProcs (get1, get2) = do
    aa <- get1 (asText functionList)
    let (ResultSet _ aa1 _) = aa
-- here I have:  RowDescription, [DataRow], CommandComplete   ===> ResultSet

    let a = map mkdbp aa1

    bb <- get2 (asText functionList)
    let (ResultSet _ bb1 _) = bb

    let b = map mkdbp bb1

    print "start dbcompare"
    let cc = dbCompare a b
    print "past dbcompare"
    
    let cnt = dcount iseq cc

    strPut $ if (fst cnt > 0) then asString sok ++ (show $ fst cnt) ++ " matches, " else ""
    strPutLn $ if (snd cnt > 0) then concat [asString $ setColor dullRed,show $ snd cnt," differences"] else concat [asString sok, "no differences"]
    strPut treset
    return $ filter (not . iseq) cc

instance Ord DbProc where
  compare a b = let hd p = map ($ p) [schema, name, argTypes] in compare (hd a) (hd b)

