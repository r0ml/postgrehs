{-# LANGUAGE  OverloadedStrings, FlexibleContexts #-}

import Preface
import PostgreSQL as PG

main :: IO ()
main = do
  args <- getArgs
  let sport:dbase:_ = args
      port = read sport :: Int

  conn <- newIORef ( Nothing, dbase)
{-
  inbox <- newEmptyMVar
  reconnect conn
  _ <- forkOS $ forever $ do
    ( req, rsp) <- takeMVar inbox -- get a request
    print ("have request", req)
    putMVar rsp =<< getsess conn req 
-}

  runSCGI port (my_main conn dbase)

readCookies :: String -> [(String,String)]
readCookies s =
    let (xs,ys) = strBrk (=='=') (trimL s)
        (zs,ws) = strBrk (==';') (trimL (strDrop 1 ys))
     in if null xs then [] else (xs,zs):readCookies (strDrop 1 ws)

-- REMOTE_ADDR and/or REMOTE_PORT ?
my_main :: IORef (Maybe PG.Postgres,String) -> String -> CGIVars -> CGI -> IO HTTPHeaders
my_main conn dbs hdrs cgir = do
  print ("my_main", hdrs)
  -- hdrs <- cgiGetHeaders cgir
  let uu = unEscapeString (fromJust $ lookup "PATH_INFO" hdrs)
      -- uu = if last uux == '/' then tail uux ++ "index.html" else tail uux
      cookies = case lookup "HTTP_COOKIE" hdrs of { Nothing -> []; Just x -> readCookies x }
      jsess = fromMaybe "" (lookup "JSESSIONID" cookies)
      qs = fromMaybe "" (lookup "QUERY_STRING" hdrs)
      Just rm = lookup "REQUEST_METHOD" hdrs
  d <- case rm of
          "POST" -> cgiGetBody cgir
          "GET" -> return strEmpty
          _ -> putStrLn ("REQUEST_METHOD: "++rm) >> return strEmpty
  putStrLn ("pre-db lookup" ++ show jsess ++ " data: " ++ show d)
  if "/login" `isSuffixOf` uu then reconnect conn jsess >> return []
                              else makeRequest conn dbs (asText d) cgir

-----------------------------------------------------------------------------------------------
-- Session stuff
-----------------------------------------------------------------------------------------------

noUser :: SessionContext -> Bool
noUser (SessionContext a) = strNull (head a)

type ReqRsp a b =  MVar (a, MVar b)
type DbRequest = String
data SessionContext = SessionContext [ByteString] | DbError ByteString

-- | This function starts a thread which communicates with the database to retrieve session information
-- databaser :: String -> IO (ReqRsp DbRequest SessionContext)

-- varchar :: Oid
-- varchar = Oid 1043

reconnect :: IORef (Maybe PG.Postgres, String) -> String -> IO ()
reconnect ior jsess = do
  (_, fsdb) <- readIORef ior
  idb <- PG.connectToDb (asText fsdb)
  -- what happens if there is a connection error?
  -- erm <- fmap (fromMaybe "") (PG.errorMessage idb)
  -- print erm
  
  -- what to do if there was an error?

  -- the next two lines are meant to establish a cursor (statement)
  -- for checking sessions
  -- PG.sendQuery idb (PG.Parse "q1" "select * from session.check_session($1, $2)" [1043, 1043])
  writeIORef ior (Just idb, fsdb)

makeRequest :: IORef (Maybe PG.Postgres, String) -> String -> Text -> CGI -> IO HTTPHeaders
makeRequest ior dbs js cgir = do
  (connx, _fsdb) <- readIORef ior
  if connx == Nothing then reconnect ior dbs else return () 
  (Just conn, fsdb) <- readIORef ior
  cc <- PG.doQuery conn (PG.Query js)

{-
  print "BIND""
  PG.doQuery conn (PG.Bind "" "q1" [Just (asByteString js), Nothing] )
  print "EXECUTE"
  PG.sendQuery conn (PG.Execute "" 1)
  print "CLOSEPORTAL"
  PG.sendQuery conn (PG.ClosePortal "")
  print "SYNC"
  cc <- PG.doQuery conn PG.Sync
-}
  putStrLn ("serving "++ show js)

  case cc of
      ErrorMessage dberr -> writeResponse cgir ("Database connection error: " ++ (show dberr)) >> return [("Status", "503 Database error"), ("Content-Type", "text/html")]
      _ -> writeResponse cgir (show cc) >> return [("Status", "200 OK"), ("Content-Type", "text/plain")]

{-
getsess :: IORef (PG.Postgres, String) -> String -> IO SessionContext
getsess iconn js = do
  print ("getsess", js)
  (conn, nret) <- readIORef iconn

  print "read IORef"
  cc <- doGetsess conn js

  print ("got sess",cc)
-- I could get an EndSession (or maybe an Error? )
  case cc of
    PG.EndSession -> do
      print ("Resetting database connection" :: String)
      _ <- reconnect iconn
      -- is this a bottomless recursion ?
      getsess iconn js

    PG.ResultSet rd fdr fz ->
      if null fdr then return $ SessionContext [ "","","","",""]
      else let dra = head fdr in return (SessionContext (map (maybe "" id) dra))

    _ -> do
      print ("unkown response from database: " ++ show cc)
      error (show cc)
-}
      
