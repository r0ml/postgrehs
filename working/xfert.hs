
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

xfer1 conn1 conn2 = do
  cr <- getCopyData conn1 False
  case cr of 
    CopyOutDone -> do
      print "copyoutdone"
      r <- putCopyEnd conn2 Nothing
      print r
      
      tt <- getResult conn2
      let ttm = fromJust tt
      zxs <- resultStatus ttm
      print zxs
      zxm <- resultErrorMessage ttm
      print zxm

      zxi <- ntuples ttm
      print zxi
      
      ct <- cmdTuples ttm
      print ct
      
      return r
    CopyOutRow rowdata -> do
      print "copying a row..."
      z <- putCopyData conn2 rowdata
      print z
      xfer1 conn1 conn2
    _ -> do
      print cr
      (errorMessage conn2) >>= (error . unpack . fromJust)

xfer :: Connection -> Connection -> String -> IO CopyInResult
xfer conn1 conn2 tbl = do
  print ("Starting copy of "++tbl)
  cc <- exec conn1 (fromString ("COPY (SELECT * FROM "++tbl++" LIMIT 3) TO STDOUT"))
  let c = fromJust cc
  rs <- resultStatus c
  if not (rs == CopyOut) then (resultErrorMessage c) >>= (error . unpack . fromJust) else return ()
  dd <- exec conn2 (fromString ("COPY "++tbl++" FROM STDIN"))
  let d = fromJust dd
  rs2 <- resultStatus d
  if not (rs2 == CopyIn) then (resultErrorMessage d) >>= (error . unpack . fromJust) else return ()
  res <- xfer1 conn1 conn2
  print (tbl ++ " copied")
  print res
  return res

main = do
  args <- getArgs
  conn <- connectdb (fromString (head args))
  b <- status conn
  print b
  
  conn2 <- connectdb ((fromString .head . tail) args )
  b2 <- status conn2
  print b2
  
  let tbl = fromString $ head $ tail $ tail args
  
  xfer conn conn2 tbl

  finish conn
  finish conn2
  
  