{-# LANGUAGE OverloadedStrings #-}

import Database.PostgreSQL.LibPQ
import System.Environment
import Data.String (fromString)
import Data.Maybe (fromJust)
import Data.ByteString.Char8 as B (concat, unpack, pack)
import Debug.Trace

mkseqcmd [x,y] = "select setval('"++x++"."++y++"',nextval('"++x++"."++y++"')-1)"
  
varchar = Oid 1043
int8 = Oid 20
int4 = Oid 23

getval conn [x,y] = do
  res <- execParams conn "select setval($1, nextval($1)-1)" [Just (varchar, B.concat [x,".",y], Text)] Text
  errm <- resultErrorMessage (fromJust res)
  print errm
  t <- getvalue (fromJust res) 0 0
  return (read (B.unpack (fromJust t)):: Int)

setval conn [x,y] z = do
  res <- execParams conn "select setval($1, $2)" [Just (varchar, B.concat [x,".",y], Text), Just (int8, B.pack (show z), Text )] Text
  errm <- resultErrorMessage (fromJust res)
  print errm
  t <- getvalue (fromJust res) 0 0
  return (read (B.unpack (fromJust t)):: Int)
  
main = do
  args <- getArgs
  conn <- connectdb (fromString (head args))
  b <- status conn
  print b
  
  cc <- exec conn "select n.nspname, c.relname from pg_class c left join pg_namespace n on n.oid=c.relnamespace where c.relkind in ('S','s','')"
  let c = fromJust cc -- a Result
  rs <- resultStatus c -- should be TuplesOK
  nt <- ntuples c
  nf <- nfields c
  
  -- z <- mapM (\x -> mapM (getvalue c x ) [0..nf-1]) [0..nt-1]
  z <- mapM (\x -> mapM (\y -> getvalue c x y >>= (return . fromJust) ) [0,1] ) [0..nt-1]
  print z
  
  y <- mapM (getval conn) z
  print y
  
  print $ zip (map (\[x,y] -> B.concat [x,".",y]) z) y
  -- select setval('ownership.transaction_tran_id_seq',nextval('ownership.transaction_tran_id_seq')-1);
  
  finish conn
  