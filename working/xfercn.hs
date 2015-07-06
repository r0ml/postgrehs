
import Database.PostgreSQL.LibPQ
import System.Environment
import Data.String (fromString)
import Data.Maybe (fromJust)
import Data.ByteString.Char8 (pack, unpack)

slist conn s = do
  cc <- exec conn (fromString s)
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == TuplesOk) then (resultErrorMessage c) >>= (error . unpack . fromJust) else return ()
  
  nt <- ntuples c
  -- nf <- nfields c -- dont need this
  z <- mapM (\x -> getvalue c x 0) [0..nt-1]
  return (map fromJust z)

sdo conn s = do
  cc <- exec conn (fromString s)
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == CommandOk) then (resultErrorMessage c) >>= (error . unpack . fromJust)
  else return ()
  z <- cmdTuples c
  return (read (unpack (fromJust z))::Int)

main = do
  args <- getArgs
  conn <- connectdb (fromString (head args))
  b <- status conn
  print b

  let tbl = head (tail args)

  cc <- exec conn (fromString ("SELECT * FROM "++tbl++" LIMIT 0"))
  let c = fromJust cc
  rs <- resultStatus c
  print rs

  if not (rs == TuplesOk) then (resultErrorMessage c) >>= (error . unpack . fromJust) else return ()
  z <- nfields c
  r <- mapM (fname c) [0..z-1]
  print (map fromJust r)
  finish conn
  
  