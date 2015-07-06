{-# LANGUAGE OverloadedStrings #-}

import Database.PostgreSQL.LibPQ
import System.Environment
import Data.String (fromString)
import Data.Maybe (fromJust)
import Data.ByteString.Char8 as B (concat, pack, unpack)
import Data.List (intercalate)

varchar = Oid 1043
int8 = Oid 20
int4 = Oid 23

slist conn s = do
  cc <- exec conn (fromString s)
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == TuplesOk) then (resultErrorMessage c) >>= (error . B.unpack . fromJust) else return ()
  
  nt <- ntuples c
  -- nf <- nfields c -- dont need this
  z <- mapM (\x -> getvalue c x 0) [0..nt-1]
  return (map fromJust z)

sdo conn s = do
  cc <- exec conn (fromString s)
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == CommandOk) then (resultErrorMessage c) >>= (error . B.unpack . fromJust)
  else return ()
  z <- cmdTuples c
  return (read (unpack (fromJust z))::Int)

fldlist conn tbl = do
  cc <- exec conn (fromString ("SELECT * FROM "++tbl++" LIMIT 0"))
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == TuplesOk) then (resultErrorMessage c) >>= (error . B.unpack . fromJust) else return ()
  z <- nfields c
  r <- mapM (fname c) [0..z-1]
  return (map (B.unpack . fromJust) r)

xfer1 conn1 conn2 = do
  cr <- getCopyData conn1 False
  case cr of 
    CopyOutDone -> do
      putCopyEnd conn2 Nothing
      ced <- getResult conn2
      let ttm = fromJust ced

      zxs <- resultStatus (fromJust ced)
      zxm <- resultErrorMessage ttm
      if not (zxs == CommandOk) then do
        print (B.unpack (fromJust zxm))
        return (-1)
      else do
        ct <- cmdTuples ttm
        return ( read (B.unpack (fromJust ct)) :: Int)
    CopyOutRow rowdata -> do 
      putCopyData conn2 rowdata
      xfer1 conn1 conn2
    _ -> do
      print cr
      (errorMessage conn2) >>= (error . B.unpack . fromJust)

xfer :: Connection -> Connection -> String -> IO Int
xfer conn1 conn2 tbl = do
  print ("Starting copy of "++tbl)
  flds <- fldlist conn2 tbl -- get the fieldlist
  let fls = intercalate "," (map (\x -> "\""++x++"\"" ) flds)
  cc <- exec conn1 (fromString ("COPY (SELECT "++fls++" FROM "++tbl++") TO STDOUT"))
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == CopyOut) then (resultErrorMessage c) >>= (error . B.unpack . fromJust) else return ()
  dd <- exec conn2 (fromString ("COPY "++tbl++" FROM STDIN"))
  let d = fromJust dd
  rs2 <- resultStatus d
  if not (rs2 == CopyIn) then (resultErrorMessage d) >>= (error . B.unpack . fromJust) else return ()
  res <- xfer1 conn1 conn2
  print (tbl ++ " copied")
  print res
  return res


deprec conn1 conn2 = do
  n4 <- sdo conn1 "create temp table zlist as select oid::regclass from tlist where oid not in (select oid from xlist) and oid not in (select conrelid from constrs where confrelid not in (select oid from xlist))"
  print ("Only constrained " ++ show n4)    

  if (n4 > 0) then do
    tbls3 <- slist conn1 "select oid::regclass from zlist"
    print tbls3
    mapM_ (xfer conn1 conn2) (map B.unpack tbls3)
    n5 <- sdo conn1 "insert into xlist select * from zlist"
    n6 <- sdo conn1 "drop table zlist"
    deprec conn1 conn2
  else return n4


getval conn [x,y] = do
  res <- execParams conn "select setval($1, nextval($1)-1)" [Just (varchar, B.concat [x,".",y], Text)] Text
  errm <- resultErrorMessage (fromJust res)
  print errm
  t <- getvalue (fromJust res) 0 0
  return (read (B.unpack (fromJust t)):: Int)

setval conn x z = do
  res <- execParams conn "select setval($1, $2)" [Just (varchar, x, Text), Just (int8, B.pack (show z), Text )] Text
  errm <- resultErrorMessage (fromJust res)
  print errm
  t <- getvalue (fromJust res) 0 0
  return (read (B.unpack (fromJust t)):: Int)

-- now transfer the sequences
getseqs conn = do
  cc <- exec conn "select n.nspname, c.relname from pg_class c left join pg_namespace n on n.oid=c.relnamespace where c.relkind in ('S','s','')"
  let c = fromJust cc -- a Result
  rs <- resultStatus c -- should be TuplesOK
  nt <- ntuples c
  nf <- nfields c
  
  -- z <- mapM (\x -> mapM (getvalue c x ) [0..nf-1]) [0..nt-1]
  z <- mapM (\x -> mapM (\y -> getvalue c x y >>= (return . fromJust) ) [0,1] ) [0..nt-1]
  y <- mapM (getval conn) z
  return $ zip (map (\[x,y] -> B.concat [x,".",y]) z) y

main = do
  (froms : tos : schemes : _) <- getArgs
  conn <- connectdb froms
  b <- status conn
  print b
  
  conn2 <- connectdb tos args )
  b2 <- status conn2
  print b2

  n1 <- sdo conn "CREATE TEMP TABLE tlist AS SELECT c.oid::regclass FROM pg_catalog.pg_class c WHERE c.relkind IN ('r') and c.relnamespace in (select oid from pg_namespace where nspname in ('" ++ schemes ++ "')) ORDER by 1"
  print ("Tables " ++ show n1)
    
  tbls <- slist conn "SELECT oid::regclass FROM tlist"
  print tbls
  
  n2 <- sdo conn "create temp table constrs as select conname,confrelid::regclass,conrelid::regclass from pg_constraint where contype='f' and conrelid in (select oid from tlist)"
  print ("Constraints " ++ show n2)
  
  n3 <- sdo conn "create temp table xlist as select oid::regclass from tlist where not oid::regclass in (select conrelid from constrs) order by 1"
  print ("Unconstrained " ++ show n3)
  
  tbls2 <- slist conn "select oid::regclass from xlist"
  print tbls2
  
  mapM_ (xfer conn conn2) (map B.unpack tbls2)

  deprec conn conn2      
  
  sqs <- getseqs conn
  print sqs
  
  ssq <- mapM (uncurry (setval conn2)) sqs
  print ssq
  
  finish conn
  finish conn2
  
  
