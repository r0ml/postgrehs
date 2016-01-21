{-# LANGUAGE QuasiQuotes, FlexibleInstances, OverloadedStrings #-}

module Trigger where

import PostgreSQL
import Preface
import Util

triggerList = [qqstr|
SELECT n.nspname as "Schema", c.relname AS "Relation", t.tgname AS "Name", tgtype AS "Type", t.tgenabled = 'O' AS enabled,
  concat (np.nspname, '.', p.proname) AS procedure,
  pg_get_triggerdef(t.oid) as definition
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid  
JOIN pg_catalog.pg_proc p ON t.tgfoid = p.oid
JOIN pg_catalog.pg_namespace np ON p.pronamespace = np.oid
WHERE t.tgconstraint = 0  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]

data TriggerWhen = After | Before | InsteadOf deriving (Show, Eq)
data TriggerWhat = Insert | Delete | Update | Truncate deriving (Show, Eq)
data TriggerType = TriggerType TriggerWhen [TriggerWhat] TriggerHow deriving (Show, Eq) 
data TriggerHow = ForEachRow | ForEachStatement deriving (Show, Eq)

mktt x = let w = if testBit x 1 then Before else if testBit x 6 then InsteadOf else After
             t = map snd $ filter (\(b,z) -> testBit x b) $ [(2,Insert), (3,Delete), (4,Update), (5,Truncate)]
             h = if testBit x 0 then ForEachRow else ForEachStatement
         in TriggerType w t h

{- tgtype is the type (INSERT, UPDATE) 
   tgattr is which column
 -}
data DbTrigger = DbTrigger { schema :: Text, relation :: Text, name :: Text, triggerType :: TriggerType, enabled :: Bool,
                             procedure :: Text, definition :: Text }
  deriving(Show)

mkdbt :: [FieldValue] -> DbTrigger
mkdbt (s : r : n : t : e : p : src : _ ) = DbTrigger {
  schema = stringField s,
  relation = stringField r,
  name = stringField n,
  triggerType = mktt (intField t),
  enabled = boolField e,
  procedure = stringField p,
  definition = stringField src
}

instance Show (Comparison DbTrigger) where
    show (Equal x) = concat [asString sok, asString $ showTrigger x, asString treset]
    show (LeftOnly a) = concat [asString azure, stringleton charLeftArrow," ", asString $ showTrigger a, asString treset]
    show (RightOnly a) = concat [asString peach, stringleton charRightArrow, " ", asString $ showTrigger a,  asString treset]
    show (Unequal a b) = concat [asString nok, asString $ showTrigger a,  asString treset, 
         if compareIgnoringWhiteSpace (definition a) (definition b) then ""
            else concat [asString $ setAttr bold,"\n  definition differences: \n", asString treset, concatMap show $ diff (asString (definition a)) (asString (definition b))]
         ]

instance Comparable DbTrigger where
  objCmp a b =
    if compareIgnoringWhiteSpace (definition a) (definition b) then Equal a
    else Unequal a b


compareTriggers :: (Text -> IO PgResult, Text -> IO PgResult) -> IO [Comparison DbTrigger]
compareTriggers (get1, get2) = do
    aa <- get1 triggerList
    let (ResultSet _ aa1 _) = aa
    print aa
    -- aac <- get1 viewColumns
    -- aat <- get1 viewTriggers
    -- aar <- get1 viewRules

    bb <- get2 triggerList
    let (ResultSet _ bb1 _) = bb
    -- bbc <- get2 viewColumns
    -- bbt <- get2 viewTriggers
    -- bbr <- get2 viewRules

    let a = map mkdbt aa1
    let b = map mkdbt bb1

    let cc = dbCompare a b
    let cnt = dcount iseq cc
    putStr $ if fst cnt > 0 then asString sok ++ show (fst cnt) ++ " matches, " else ""
    putStrLn $ if snd cnt > 0 then concat [asString $ setColor dullRed,show $ snd cnt," differences"] else concat [asString sok,"no differences"]
    putStr (asString treset)
    return $ filter (not . iseq) cc

showTrigger x = strConcat [schema x, ".", relation x, "." , name x]

instance Ord DbTrigger where
  compare a b = let hd p = map ($ p) [schema, relation, name] in compare (hd a) (hd b)

instance Eq DbTrigger where
  (==) a b = EQ == compare a b
