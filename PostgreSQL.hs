{-# LANGUAGE OverloadedStrings, FlexibleInstances, TypeSynonymInstances #-}

module PostgreSQL (connectToDb, disconnect, sendQuery, doQuery, getNextResult,
                   Postgres, PgMessage(..), PgResult(..),
                   FieldValue, stringField, intField, boolField,
                   PgValue, fromPg
                  )
where

import Preface.R0ml hiding(try)

-- | this has the connectivity channels and the result from the connection attempt (so one can tell if
--        reconnection is required)
data PostgresR = PostgresR PostgresX PgResult (ThreadId, Chan PgMessage) (ThreadId, Chan PgMessage)
type Postgres = IORef (Maybe PostgresR)
                       
data PostgresX = PostgresX { pgSend :: Chan PgQuery, pgRecv :: Chan PgResult,
                             pgConnInfo :: PgConnInfo }

data PgConnInfo = PgConnInfo Text Text Int Text Text Text -- origstring, host, port, dbname, userid, password
  deriving (Show)
           
data PgResult =
        ResultSet (Maybe RowDescriptionT) [DataRowT] Text
      | ErrorMessage [(Char, Text)]
      | EndSession
      | OtherResult [PgMessage]
    deriving (Show)

type PgQuery = PgMessage

data PgMessage = StartUpMessage [(Text,Text)] 
    | Bind Text Text [FieldValue]
    | CancelRequest Int Int
    | ClosePortal Text
    | CloseStatement Text
    | CopyData ByteString
    | CopyDone
    | CopyInResponse Bool [Bool]
    | CopyOutResponse Bool [Bool]
    | CopyBothResponse Bool [Bool] 
    | CopyFail Text
    | DescribePortal Text
    | DescribeStatement Text
    | Execute Text Int
    | Flush
--    | FunctionCall Oid [FieldValue]  -- considered legacy
    | Parse Text Text [DataType]
    | Password Text Text ByteString -- might be MD5'd
    | Query Text
    | SSLRequest
    | Sync
    | Terminate
    | Authentication Int ByteString
    | ParameterStatus Text Text
    | BackendKey Int Int
    | RowDescription RowDescriptionT
    | DataRow DataRowT
    | FunctionResult FieldValue
    | NoticeResponse [(Char,Text)]
    | Notification Int Text Text
    | CloseComplete
    | EmptyQuery
    | NoData
    | ReadyForQuery Char
    | ParseComplete
    | BindComplete
    | PortalSuspended
    | ErrorResponse [(Char,Text)]
    | CommandComplete Text
    deriving (Show)

class PgValue a where
  fromPg :: FieldValue -> a

instance PgValue Int where
  fromPg = read . asString . fromMaybe "0"

instance PgValue Text where
  fromPg = asText . fromMaybe ""

instance PgValue Bool where
  fromPg = (== "t") . asString . fromMaybe "f" 

instance PgValue a => PgValue [a] where
  fromPg (Just x) = map (fromPg . Just . asByteString) (tpl (asText x)) 
  fromPg Nothing = undefined 

quoted_string :: Text -> (Text, Text)
quoted_string t = let Just z = strElemIndex '"' (strDrop 1 t)
                   in if 2+z >= strLen t || '"' /= strHead (strDrop (2+z) t)
                           then (strTake z (strDrop 1 t), strDrop (2+z) t)
                           else let (rq, rs) = quoted_string (strDrop (2+z) t)
                                 in (strCat [strTake z (strDrop 1 t), rq], rs) 

tpl :: Text -> [Text]
tpl t = let t1 = strTail t -- first char is '(' or ','
            (t2,t3) = if strHead t1 == '"' then quoted_string t1
                      else strBreak (\x -> x ==',' || x == ')') t1
         in if strNull t1 || strHead t1 == ')' then [] else t2 : tpl t3

-- need to handle quoted strings (from Acl.hs)

-- tupleField :: FieldValue -> 

stringField :: FieldValue -> Text
stringField = asText . fromMaybe ""

intField :: FieldValue -> Int
intField = read . asString . fromMaybe "0"

boolField :: FieldValue -> Bool
boolField = (== "t") . asString . fromMaybe "f"

  
-- type Parameter = String -- placeholder
-- type Argument = String -- placeholder
type DataType = Int -- placeholder
data FieldDef = FieldDef Text Int Int Int Int Int Int deriving (Show) -- placeholder
type FieldValue = Maybe ByteString -- placeholder
type Oid = Int -- placeholder ?

type RowDescriptionT = [FieldDef]
type DataRowT = [FieldValue]

-- data ResultSet = ResultSet RowDescriptionT [DataRowT] String

-- newtype Severity = Error | Fatal | Panic | Warning | Notice | Debug | Info | Log

printMsg :: String -> IO ()
-- printMsg a = return () 

printMsg x = putStrLn (csi [38, 2, 20, 20, 20 :: Int] "m" ++ x ++ treset)
  where treset = "\ESC[m"
        csi args code = concat ["\ESC[", intercalate ";" (map show args), code]
  
getFieldData :: ByteStream -> (ByteStream, Maybe ByteString)
getFieldData (ByteStream s p) = let a = getInt32 p s
  in if a == -1 then (ByteStream s (p+4), Nothing)
     else (ByteStream s (p+4+a), Just (strTake a (strDrop (4+p) s)))

getUntil0 :: ByteStream -> (ByteStream, ByteString)
getUntil0 (ByteStream s n) =
  case strElemIndex (0::Word8) (strDrop n s) of
     Nothing -> (ByteStream s (strLen s), strDrop n s)
     Just x ->  (ByteStream s (n+x+1), strTake x (strDrop n s))

data ByteStream = ByteStream ByteString Integer

getMessageComponent :: ByteStream -> (ByteStream,Char,Text)
getMessageComponent (ByteStream ss nn) =
  let n = nth ss nn
      (bb,s) = if n == 0 then (ByteStream ss (nn+1), "")
               else (getUntil0 (ByteStream ss (nn+1)) )
   in (ByteStream ss (nn+strLen s+(if n == 0 then 1 else 2)), chr (fromIntegral n) , asText s )
    
getMessageComponents :: ByteStream -> (ByteStream, [(Char, Text)])
getMessageComponents bs = getMessageComponents' bs []
  where getMessageComponents' :: ByteStream -> [(Char,Text)] -> (ByteStream, [(Char, Text)])
        getMessageComponents' bs a = 
            let (b, c, z) = getMessageComponent bs
             in if c == '\0' then (b, a) else getMessageComponents' b ((c,z) : a)

getFieldDef :: ByteStream -> (ByteStream, FieldDef)
getFieldDef sa@(ByteStream s n) = 
  let (ByteStream s2 v, a) = getUntil0 sa
      b = getInt32 v s2
      c = getInt16 (v+4) s2
      d = getInt32 (v+6) s2
      e = getInt16 (v+10) s2
      f = getInt32 (v+12) s2
      g = getInt16 (v+16) s2
   in (ByteStream s2 (v+18), FieldDef (asText a) b c d e f g)

getMsg :: Char -> ByteString -> PgMessage
getMsg 'R' s = let au = getInt32 0 s
                in case au of
                     0 -> Authentication 0 zilde
                     3 -> Authentication 3 zilde
                     5 -> let md = strTake 4 (strDrop 4 s) in Authentication 5 md
                     _ -> Authentication au zilde
getMsg 'S' s = let a = strTake (strLen s - 1) s  
                   [p,q] = splitStr "\000" a
                in ParameterStatus (asText p) (asText q)

getMsg 'K' s = BackendKey (getInt32 0 s) (getInt32 4 s)

getMsg 'Z' s = (ReadyForQuery . chr . fromIntegral . strHead) s

getMsg 'T' s = let flds = getInt16 0 s -- the number of fields
                in RowDescription ((take flds . snd . unzip) (iterate (\(b,n)->getFieldDef b) (getFieldDef (ByteStream s 2) )))

getMsg 'D' s = let flds = getInt16 0 s -- the number of fields
              in DataRow ((take flds . snd . unzip) (iterate (\(b,n)->getFieldData b) (getFieldData (ByteStream s 2))))
getMsg 'C' s = let a = strTake (strLen s - 1) s in CommandComplete (asText a)
getMsg 'V' s = let (bs, r) = getFieldData (ByteStream s 0) in FunctionResult r
getMsg 'E' s = let (bs, r) = getMessageComponents (ByteStream s 0) in ErrorResponse r
getMsg '1' s = assert (strLen s == 0) ParseComplete
getMsg '2' s = assert (strLen s == 0) BindComplete
getMsg '3' s = assert (strLen s == 0) CloseComplete
getMsg 'd' s = CopyData (strTake (strLen s - 1) s)
getMsg 'c' s = assert (strLen s == 0) CopyDone
getMsg 's' s = assert (strLen s == 0) PortalSuspended
getMsg 'I' s = assert (strLen s == 0) EmptyQuery
getMsg 'n' s = assert (strLen s == 0) NoData
getMsg 'N' s = let (bs, r) = getMessageComponents (ByteStream s 0) in NoticeResponse r
getMsg 'A' s = let prc = getInt32 0 s
                   (ns, chan) = getUntil0 (ByteStream s 4)
                   (ns2, pay) = getUntil0 ns2
                in Notification prc (asText chan) (asText pay)
getMsg 't' _s = undefined -- ParameterDescription 
getMsg 'X' _s = Terminate -- never really sent by the backend, but pseudo-sent when the socket is closed
getMsg _ _ = undefined

putMsg :: PgMessage -> ByteString
putMsg (Query s) = strCat [ putByte 'Q', putStringMessage s]

putMsg (StartUpMessage a ) = 
     let m = concatMap (\(x,y) -> [asByteString x, asByteString y] ) a
         j = 9 + foldl (\x y -> x + 1 + strLen y) 0 m
      in strCat $ [int32 (fromInteger j), int32 protocolVersion] ++ (map hString m) ++ [zero]
putMsg Terminate = strCat [putByte 'X', putWord32be 4]
putMsg Sync = strCat [putByte 'S', putWord32be 4]
putMsg Flush = strCat [putByte 'H', putWord32be 4]
putMsg SSLRequest = strCat [ putByte 'F',  putWord32be 8, putWord32be 80877103]
putMsg (Password p u b) = let rp = strCat ["md5" , asByteString . stringDigest $ md5 ( strCat [ asByteString . stringDigest $ md5 (strCat [asByteString p, asByteString u]), b] )] 
                          in strCat [putByte 'p', putWord32be ( fromIntegral (strLen rp + 5)), rp, zero]

{- -- considered legacy
      put (FunctionCall oid fvs) = do
      putByte 'F'
      putWord32be (fromIntegral ml)
      putWord32be (fromIntegral oid)
      putWord16be 0 -- all arguments are text
      putWord16be (fromIntegral (length fvs))
      putByteString args
      putWord16be 0 -- result is text
    where args = asByteString (runPut (mapM_ putFieldValue fvs))
          ml = 14 + strLen args
-}

putMsg (Execute port mx) = 
      strCat [putByte 'E', putWord32be (fromIntegral ml), namb, zero, 
      putWord32be (fromIntegral mx)]
    where namb = asByteString port
          ml = 9 + strLen namb
  
putMsg (Parse nam stmt dts) = strConcat ([
      putByte 'P', putWord32be (fromIntegral ml),
      namb, zero, stmtb, zero, putWord16be (fromIntegral (length dts))]
      ++ map (putWord32be . fromIntegral) dts )
    where namb = asByteString nam
          stmtb = asByteString stmt
          ml = (4* length dts) + 8 + fromInteger (strLen namb + strLen stmtb)

putMsg (Bind nam stmt vals) = strCat ( [
      putByte 'B', putWord32be (fromIntegral ml),
      namb, zero, stmtb, zero, 
      putWord16be 0, -- all use text
      putWord16be (fromIntegral (length vals))] ++ 
      args ++ [ putWord16be 0 ]) -- all use text
    where args = map putFieldValue vals
          namb = asByteString nam
          stmtb = asByteString stmt
          ml = strLen namb + strLen stmtb + 12 + (sum (map strLen args))
  
putMsg (CancelRequest prc key) = 
  strConcat [ putByte 'F', putWord32be 16, 
      putWord32be 80877102, putWord32be (fromIntegral prc),
      putWord32be (fromIntegral key)]
  
putMsg (ClosePortal x) = dcp 'C' 'P' x
putMsg (CloseStatement x) = dcp 'C' 'S' x

putMsg (CopyData x) = undefined
  
putMsg CopyDone = strConcat [ putByte 'c' ,  putWord32be 4 ]
  
putMsg (CopyFail x) = strConcat [
      putByte 'f', putWord32be (fromIntegral ml),  a, zero]
    where a = asByteString x
          ml = strLen a + 5
  
putMsg (DescribePortal x) = dcp 'D' 'P' x
putMsg (DescribeStatement x) = dcp 'D' 'S' x
    
putMsg _ = undefined

putStringMessage :: Text -> ByteString
putStringMessage s = 
  let sb = asByteString s
      len = putWord32be ( fromIntegral (strLen sb + 5))
   in strConcat [len, sb, zero]

zero :: ByteString
zero = stringleton (0::Word8)

putByte :: Char -> ByteString
putByte = stringleton . fromIntegral . fromEnum

putFieldValue :: Maybe ByteString -> ByteString
putFieldValue fv = case fv of 
    Nothing -> putWord32be (fromIntegral (-1 :: Int))
    Just b -> strConcat [ putWord32be (fromIntegral (strLen b)) , b ]

-- describe portal
dcp :: Char -> Char -> Text -> ByteString
dcp a b x = 
  let nam = asByteString x
   in strConcat [ putByte a, putWord32be (fromIntegral (6 + strLen nam)), putByte b, nam, zero]

-- | A field size.
data Size = Varying | Size Int16
  deriving (Eq,Ord,Show)

-- | A text format code. Will always be TextCode for DESCRIBE queries.
data FormatCode = TextCode | BinaryCode
  deriving (Eq,Ord,Show)

-- | A PostgreSQL object ID.
-- type ObjectId = Int

loop :: IO () -> IO ()
loop f = f >> loop f

doUntil :: IO Bool -> IO ()
doUntil f = f >>= (`unless` doUntil f)

       -- getDataRow x = readChan x >>= return . (\y -> case y of { DataRow d -> Right (d :: DataRowT) ; e@_ -> Left e } )

-- getResponse :: Postgres -> IO PgResult
-- getResponse (Postgres a x) = readChan x

sendQuery :: Postgres -> PgQuery -> IO ()
sendQuery pg q = do
  PostgresR (PostgresX z _ _) _ _ _  <- reconnect pg
  writeChan z q

{-
unfoldWhile :: IO (Either b a) -> IO ([a],b)  -- the list of "whiles" and the terminator
unfoldWhile m = xloop 
    where xloop = m >>= \e -> case e of { Right x -> xloop >>= (\(xs, r) -> return (x:xs, r)); Left y -> return ([],y) } 
-}

doQuery :: Postgres -> PgQuery -> IO PgResult
doQuery pg s = do { PostgresR (PostgresX z x _) _ _ _ <- reconnect pg;  writeChan z s >> readChan x }

getNextResult :: Postgres -> IO PgResult
getNextResult pg = do { PostgresR (PostgresX _ x _) _ _ _ <- reconnect pg; readChan x }

sendReq :: Chan PgMessage -> Socket -> {- Chan PgMessage -> -} IO Bool
sendReq c s {- rc -} = do
  q <- readChan c
  printMsg ("sending " ++ show q)

  sendBlock s q

  -- Cancel, Close, Copyxxx, Describe, Execute, FunctionCall, Parse, Sync, Terminate
  case q of 
    Query _ -> do
      sendBlock s Flush
      return False
    Terminate -> do
      sClose s
      return True

--     StartUpMessage a b -> do 
--       let xq = [(maybe "" id $ lookup "user" a),(maybe "" id $ lookup "password" b)]    
--       processResponse s rc xq
--     Parse _ _ _ -> do
--       sendBlock s Flush
--       print "sent parse"
--     Bind _ _ _ -> do
--       sendBlock s Flush
--      print "sent bind"
    Execute _ _ -> do
       sendBlock s Flush
       return False
--        print "sent exec"

    _ -> return False

-- I should move this to the outer ring
processResponse :: Chan PgMessage -> Socket -> (Text,Text) -> IO Bool
processResponse rc s (uid,pwd) = do
  a <- readResponse s
  printMsg ("receiving "++ show a)
  case a of
    Authentication 5 bx -> do
       sendBlock s (Password pwd uid bx)
       return False
    Terminate -> writeChan rc a >> return True
--       processResponse s rc xq
--    Authentication 0 _ -> processResponse s rc xq
    _ -> writeChan rc a >> return False
--          case a of 
            -- response can be parsecomplete or error (for parse)
            -- response can be bindcomplete or error (for parse)
            -- query responses will always end with readyforquery
            -- ReadyForQuery _ -> done
            -- PortalSuspended -> done
            -- ErrorResponse _ -> done
            -- CommandComplete _ -> done
--            _ -> return () -- processResponse s rc xq

reallyRead :: Socket -> Int -> IO ByteString
reallyRead s len = do
  msg <- sktRecv s len
  let lm = fromInteger (strLen msg)
  if lm == len then return msg
  else if lm == 0 then error "really read read nothing"
  else do
    m2 <- reallyRead s (len - lm)
    return $ strCat [msg, m2]

readResponse :: Socket -> IO PgMessage
readResponse s = do
    more <- catch (reallyRead s 5) ( (\x -> do {printMsg (show x) ; return zilde} ) :: SomeException -> IO ByteString) 
    if strNull more then return Terminate
    else do msg <- reallyRead s (( getInt32 1 more ) - 4 )
            return $ getMsg ( (chr . fromEnum) (strHead more)) msg

protocolVersion :: Int
protocolVersion = 0x30000

-- | Put a Haskell string, encoding it to UTF-8, and null-terminating it.
hString :: ByteString -> ByteString
hString s = strAppend s zero

-- | Put a Haskell 32-bit integer.
int32 :: Int -> ByteString 
int32 = putWord32be . fromIntegral 

getWord32be :: Integer -> ByteString -> Int32
getWord32be n s = (fromIntegral (nth s n) `shiftL` 24) .|.
              (fromIntegral (nth s (n+1) ) `shiftL` 16) .|.
              (fromIntegral (nth s (n+2) ) `shiftL`  8) .|.
              (fromIntegral (nth s (n+3) ))

getWord16be :: Integer -> ByteString -> Int16
getWord16be n s = (fromIntegral (nth s n) `shiftL` 8) .|.
              (fromIntegral (nth s (n+1) ))

getInt32 :: Integral a => Integer -> ByteString -> a
getInt32 = (fromIntegral .) . getWord32be

getInt16 :: Integral a => Integer -> ByteString -> a
getInt16 = (fromIntegral .) . getWord16be

putWord32be :: Int32 -> ByteString
putWord32be w = pack [(fromIntegral (shiftR w 24) ) ,
                      (fromIntegral (shiftR w 16) ) ,
                      (fromIntegral (shiftR w  8) ) ,
                      (fromIntegral w) ]

putWord16be :: Int16 -> ByteString
putWord16be w = pack [(fromIntegral (shiftR w 8)), fromIntegral w]

-- | Send a block of bytes on a handle, prepending the complete length.
sendBlock :: Socket -> PgMessage -> IO ()
sendBlock h outp = sendAll h (asByteString (putMsg outp))
  where sendAll j msg = do
           n <- sktSend j msg
           if n <= 0 then error "failed to send"
           else let rm = strDrop (toInteger n) msg in unless (strNull rm) (sendAll j rm)

--------------------------------------------------------------------------------
-- Connection

connectToDb :: Text -> IO Postgres
connectToDb conns = do
    let [h,p,un,db]= dlml conns
        pk = read (asString p) :: Int
    pw <- fmap (fromMaybe "") (passwordFromPgpass h pk db un)
    let smp = [("user" :: String, un),("database",db)]

    rr <- connectTo (PgConnInfo conns h pk db un pw)
    conn <- newIORef (Just rr)
    reconnect conn -- try to connect now -- earlier error message potential
    return conn

disconnect :: Postgres -> IO ()
disconnect conn = do
  a <- readIORef conn
  case a of 
     Nothing -> return ()
     Just (PostgresR px pgr (ps, qs) (pr,qr)) -> do
          killThread ps
          killThread pr
          writeIORef conn Nothing

reconnect :: Postgres -> IO PostgresR
reconnect conn = do
  prp@(Just (PostgresR px pgr (ps,qs) (pr,qr))) <- readIORef conn
  case pgr of
    EndSession -> do
      killThread ps
      killThread pr
      
      sock <- socket AF_INET Stream defaultProtocol

      let PostgresX z x i = px
      let PgConnInfo _ host port db un pwd = i

      let hints = defaultHints {addrFamily = AF_INET, addrSocketType = Stream}
      addrInfos <- getAddrInfo (Just hints) (Just (asString host)) (Just $ show port)
      _connq <- catch (sktConnect sock (addrAddress $ head addrInfos) >> return True )
                     (\j -> printMsg (show (j::SomeException)) >> return False)

      p1 <- forkIO $ doUntil $ sendReq qs sock
      p2 <- forkIO $ doUntil $ processResponse qr sock (un,pwd)

      writeChan z (StartUpMessage [("user", un), ("database", db)])
      res <- readChan x
      printMsg ("Connection: "++ show res)
      let wr = PostgresR px res (p1, qs) (p2, qr)
      writeIORef conn (Just wr)
      return wr
    _ -> return (fromJust prp)

connectTo :: PgConnInfo -> IO PostgresR -- host port path headers 
connectTo ci@(PgConnInfo _ _host _port _db _uid _pwd) = do
    qchan <- newChan  -- requests
    rchan <- newChan  :: IO (Chan PgMessage) -- responses
    
    -- I'll have to figure out how to handle socket closing
    -- Create and connect socket
    -- sock      <- S.socket S.AF_INET S.Stream S.defaultProtocol

    -- this pair of threads takes outbound messages and writes them to a socket,
    -- and takes inbound messages and writes them to a channel
    -- (i.e., conversion from channel to socket and back)

    -- if either one fails, it should mark the connection as EndSession
    -- initially, nothing happens, because the connection waits until a "reconnect"
    p1 <- forkIO $ loop $ threadDelay 1000000000 -- doUntil $ sendReq qchan sock
    p2 <- forkIO $ loop $ threadDelay 1000000000 -- doUntil $ processResponse rchan sock (uid,pwd)

    -- this process reads and writes to the socket from a pair of request/response channels
    -- _ <- forkIO $ finally
    --        (do { ; loop $ sendReq qchan sock rchan })
    --        (S.sClose sock)

    achan <- newChan
    bchan <- newChan
    _ <- forkIO $ loop $ do
          q <- readChan achan
          writeChan qchan q

    _ <- forkIO $ loop $ do
          r <- getResults rchan ([],[],[])
          writeChan bchan r
    return $ PostgresR (PostgresX achan bchan ci) EndSession (p1, qchan) (p2, rchan)
    
  where getResults x g@(daccum, oaccum, saccum) = do
                    a <- readChan x
                    case a of 
                      Terminate -> return EndSession
                      ParseComplete -> getResults x g-- this means ignore the ParseComplete
                      BindComplete -> getResults x g
                      CloseComplete -> getResults x g
                      DataRow d -> getResults x (d : daccum, oaccum, saccum)
                      PortalSuspended -> return $ ResultSet Nothing (reverse daccum) "Portal Suspended"
                      RowDescription rd -> getResults x (daccum, rd : oaccum, saccum)
                      CommandComplete s -> let z = if null oaccum then Nothing else Just $ head oaccum
                                            in return $ ResultSet z (reverse daccum) s
                      ErrorResponse r -> return $ ErrorMessage r
                      ReadyForQuery _z -> if (not . null) saccum then return $ OtherResult (reverse saccum)
                                                                 else return $ OtherResult (reverse saccum) -- getResults x g
                      -- do
--                        (bs, cc) <- unfoldWhile (getDataRow x)
--                        let cx = case cc of { CommandComplete z -> z; _ -> show cc }
--                         getResults x (ResultSet (Just rd) bs cx : accum)
                      _ -> getResults x (daccum, oaccum, a : saccum)

--------------------------------------------------------------------------------
-- Connection String
kvpairs :: Text -> [(Text,Text)]
kvpairs t = let t1 = stripStart t
                (n,r) = strBreak isSpace t1
                (k,v) = strBreak (\x -> x == '=' || x == ':') n
             in if strNull t1 then [] else (asText k, asText (strDrop 1 v)) : kvpairs r

dlml :: Text -> [Text]
dlml x =
  let z = kvpairs x
      u = asText (unsafePerformIO (fromJust <$> lookupEnv "USER"))
      a = lookupWithDefault "localhost" "host" z
      b = lookupWithDefault "5432" "port" z
      c = lookupWithDefault u "user" z
      d = lookupWithDefault "" "dbname" z
   in [a,b,c,d]

--------------------------------------------------------------------------------
-- pgpass support
xtail :: Stringy a => a -> a
xtail x = if strNull x then x else strTail x

parsePgpassLine :: Text -> (Text,Int,Text,Text,Text)
parsePgpassLine a =
  let (h,z) = strBreak (==':') a
      (p,y) = strBreak (==':') (xtail z)
      nm = (reads (asString p) :: [(Int, String)])
      p2 = if null nm then 0 else (fst . head) nm
      (d,x) = strBreak (==':') (xtail y)
      (u,v) = strBreak (==':') (xtail x)
      (pw,_zz) = strBreak (==':') (xtail v)
   in (h,p2,d,u,pw)


valine :: Text -> Bool
valine a = let z = stripStart a
            in not (strNull z) && (strHead z /= '#')

passwordFromPgpass :: Text -> Int -> Text -> Text -> IO (Maybe Text)
passwordFromPgpass h p dn uid = do
    hm <- getHomeDirectory
    a <- strReadFile (hm </> ".pgpass")
    let b = map parsePgpassLine (filter valine (splitStr ("\n" :: Text) a))
    let c = filter (pgmatch h p dn uid) b
    return (if null c then Nothing else let (_,_,_,_,r) = head c in Just r)
  where pgmatch k w dx u (k',w',dx',u',_) = k' == k && w' == w && ( dx' == "*" || dx' == dx ) && u' == u

--------------------------------------------------------------------------------

