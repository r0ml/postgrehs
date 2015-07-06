{-# LANGUAGE OverloadedStrings #-}

import Database.PostgreSQL.LibPQ
import System.Environment
import Data.String (fromString)
import Data.Maybe (fromJust)

main = do
  args <- getArgs
  conn <- connectdb (fromString (head args))
  b <- status conn
  print b
  
  cc <- exec conn "select * from pg_user"
  let c = fromJust cc
  print c
  
  rs <- resultStatus c
  print rs
  
  nt <- ntuples c
  print nt

  nf <- nfields c
  print nf
    
  r <- getvalue c 0 0 
  print r
  
  z <- mapM (\x -> mapM (getvalue c x ) [0..nf-1]) [0..nt-1]
  let zz = map (map fromJust) z
  print zz
  
  finish conn
  
