{-# LANGUAGE OverloadedStrings, FlexibleInstances, TypeSynonymInstances, OverlappingInstances #-}

module PostgreSQL (connectToDb, sendQuery, doQuery, getNextResult,
                   Postgres(..), PgMessage(..), PgResult(..),
                   FieldValue, stringField, intField, boolField,
                   PgValue, fromPg
                  )
where

import Preface
import MD5

import Text.ParserCombinators.Parsec hiding ((<|>))

-- import qualified Network.Socket.ByteString as S (recv, send)
-- import qualified Network.Socket as S (sClose, Socket(..),
--   addrAddress, connect, defaultProtocol, SocketType(..), Family(..), getAddrInfo, defaultHints,
--   socket, addrSocketType, addrFamily ) 

-- import qualified Data.ByteString.Lazy as BL (toStrict)
import qualified Data.ByteString.Builder as BB

import qualified Data.ByteString as B (split)

-- | this has the connectivity channels and the result from the connection attempt (so one can tell if
--        reconnection is required)
data PostgresR = PostgresR PostgresX PgResult (ThreadId, Chan PgMessage) (ThreadId, Chan PgMessage)
type Postgres = IORef PostgresR
                       
data PostgresX = PostgresX { pgSend :: Chan PgQuery, pgRecv :: Chan PgResult,
                             pgConnInfo :: PgConnInfo }

data PgConnInfo = PgConnInfo String String Int String String String -- origstring, host, port, dbname, userid, password
  deriving (Show)
           
data PgResult =
        ResultSet (Maybe RowDescriptionT) [DataRowT] String
      | ErrorMessage [(Char, String)]
      | EndSession
      | OtherResult [PgMessage]
    deriving (Show)

type PgQuery = PgMessage

data PgMessage = StartUpMessage [(String,String)] 
    | Bind String String [FieldValue]
    | CancelRequest Int Int
    | ClosePortal String
    | CloseStatement String
    | CopyData ByteString
    | CopyDone
    | CopyInResponse Bool [Bool]
    | CopyOutResponse Bool [Bool]
    | CopyBothResponse Bool [Bool] 
    | CopyFail String
    | DescribePortal String
    | DescribeStatement String
    | Execute String Int
    | Flush
    | FunctionCall Oid [FieldValue]
    | Parse String String [DataType]
    | Password String String ByteString -- might be MD5'd
    | Query String
    | SSLRequest
    | Sync
    | Terminate
    | Authentication Int ByteString
    | ParameterStatus String String
    | BackendKey Int Int
    | RowDescription RowDescriptionT
    | DataRow DataRowT
    | FunctionResult FieldValue
    | NoticeResponse [(Char,String)]
    | Notification Int String String
    | CloseComplete
    | EmptyQuery
    | NoData
    | ReadyForQuery Char
    | ParseComplete
    | BindComplete
    | PortalSuspended
    | ErrorResponse [(Char,String)]
    | CommandComplete String
    deriving (Show)

class PgValue a where
  fromPg :: FieldValue -> a

instance PgValue Int where
  fromPg = read . asString . fromMaybe "0"

instance PgValue String where
  fromPg = asString . fromMaybe ""

instance PgValue Bool where
  fromPg = (== "t") . asString . fromMaybe "f" 

instance PgValue a => PgValue [a] where
  fromPg x = let y = parse tpl "" (maybe "" asString x) in case y of { Left z -> error (show z); Right z -> map (fromPg . Just . asByteString) z } 

dquote = char '"' <?> "double quote"
quoted_char = try (do
    _ <- char '\\'
    r <- char '"'
    return r
  <?> "quoted_char" )
                 
qtext = many( quoted_char <|> noneOf "\"")
quoted_string = do 
    _ <- dquote
    r <- qtext
    _ <- dquote
    return r
  <?> "quoted string"

qst :: CharParser () String
qst = quoted_string <|> many (noneOf ",)")

tpl :: CharParser () [String]
tpl = do 
  _ <- char '('
  t <- sepBy qst (char ',')
  _ <- char ')'
  return t

-- need to handle quoted strings (from Acl.hs)

-- tupleField :: FieldValue -> 

stringField :: FieldValue -> String
stringField = asString . fromMaybe ""

intField :: FieldValue -> Int
intField = read . asString . fromMaybe "0"

boolField :: FieldValue -> Bool
boolField = (== "t") . asString . fromMaybe "f"

  
-- type Parameter = String -- placeholder
-- type Argument = String -- placeholder
type DataType = Int -- placeholder
data FieldDef = FieldDef String Int Int Int Int Int Int deriving (Show) -- placeholder
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
  
getFieldData :: Int -> Get (Maybe ByteString)
getFieldData _n = do
  a <- getInt32
  if a == -1 then return Nothing else do
    b <- getByteString (fromIntegral a)
    return (Just b)
    

getUntil :: Word8 -> Get ByteString
getUntil c = fmap (asByteString  . BB.toLazyByteString) (getUntil' c mempty)
  where getUntil' cx a = do
          n <- getWord8
          if n == cx then return a else getUntil' cx (a `mappend` BB.word8 n)

getMessageComponent :: Get (Char,String)
getMessageComponent = do
    n <- getWord8
    s <- if n == 0 then return "" else do { b <- getUntil 0; return (asString b) } 
    return ( chr (fromIntegral n) , s )
    
getMessageComponents :: Get [(Char, String)]
getMessageComponents = getMessageComponents' []
  where getMessageComponents' a = do
            c <- getMessageComponent
            if fst c == '\0' then return a else getMessageComponents' (c : a)

getFieldDef :: a -> Get FieldDef
getFieldDef _n = do
  a <- getUntil 0
  b <- getInt32
  c <- getInt16
  d <- getInt32
  e <- getInt16
  f <- getInt32
  g <- getInt16
  return $ FieldDef (asString a) b c d e f g

instance Binary PgMessage where
  get = do
    b <- fmap (chr . fromIntegral) getWord8
    len <- getInt32 
    case b of 
      'R' -> do 
        au <- getInt32
        case au of
          0 -> return $ Authentication 0 zilde
          3 -> return $ Authentication 3 zilde
          5 -> do
            md <- getByteString 4
            return $ Authentication 5 md
          _ -> return $ Authentication au zilde
      'S' -> do 
          a <- getByteString (len - 5)
          let [p,q] = B.split 0 a
          return $ ParameterStatus (asString p) (asString q)
      'K' -> BackendKey <$> fmap fromIntegral getWord32be <*> fmap fromIntegral getWord32be
      'Z' -> ReadyForQuery . chr . fromIntegral <$> getWord8
      'T' -> do
          flds <- getInt16 -- the number of fields
          RowDescription <$> mapM getFieldDef [1..flds]
      'D' -> do
          flds <- getInt16 -- the number of fields
          DataRow <$> mapM getFieldData [1..flds]
      'C' -> do
          a <- getByteString (len - 5)
          return $ CommandComplete (asString a)
      'V' -> FunctionResult <$> getFieldData 1
      'E' -> ErrorResponse <$> getMessageComponents
      '1' -> return ParseComplete
      '2' -> return BindComplete
      '3' -> return CloseComplete
      'd' -> CopyData <$> getByteString (len - 4)
      'c' -> return CopyDone
      's' -> return PortalSuspended
      'I' -> return EmptyQuery
      'n' -> return NoData
      'N' -> NoticeResponse <$> getMessageComponents
      'A' -> do
          prc <- fmap fromIntegral getWord32be
          chan <- getUntil 0
          pay <- getUntil 0
          return (Notification prc (asString chan) (asString pay))
      't' -> undefined -- ParameterDescription 
      'X' -> return Terminate -- never really sent by the backend, but pseudo-sent when the socket is closed
      _ -> undefined

  put (Query s) = putByte 'Q' >> putStringMessage s

  put (StartUpMessage a ) = do
     let m = concatMap (\(x,y) -> [asByteString x, asByteString y] ) a
         j = 9 + foldl (\x y -> x + 1 + strLen y) 0 m
     int32 (fromInteger j)
     int32 protocolVersion
     mapM_ hString m
     zero

  put Terminate = putByte 'X' >> putWord32be 4
  put Sync = putByte 'S' >> putWord32be 4
  put Flush = putByte 'H' >> putWord32be 4
  put SSLRequest = putByte 'F' >> putWord32be 8 >> putWord32be 80877103
  put (Password p u b) = let rp = strCat ["md5", stringMD5 $ md5 ( strCat [ stringMD5 $ md5 (strCat [asByteString p, asByteString u]), b] )] 
                          in putByte 'p' >> putWord32be ( fromIntegral (strLen rp + 5)) >> putByteString rp >> zero

  put (FunctionCall oid fvs) = do
      putByte 'F'
      putWord32be (fromIntegral ml)
      putWord32be (fromIntegral oid)
      putWord16be 0 -- all arguments are text
      putWord16be (fromIntegral (length fvs))
      putByteString args
      putWord16be 0 -- result is text
    where args = asByteString (runPut (mapM_ put fvs))
          ml = 14 + strLen args

  put (Execute port mx) = do
      putByte 'E'
      putWord32be (fromIntegral ml)
      putByteString namb
      zero
      putWord32be (fromIntegral mx)
    where namb = asByteString port
          ml = 9 + strLen namb
  
  put (Parse nam stmt dts) = do
      putByte 'P'
      putWord32be (fromIntegral ml)
      putByteString namb >> zero
      putByteString stmtb >> zero
      putWord16be (fromIntegral (length dts))
      mapM_ (putWord32be . fromIntegral) dts
    where namb = asByteString nam
          stmtb = asByteString stmt
          ml = (4* length dts) + 8 + fromInteger (strLen namb + strLen stmtb)

  put (Bind nam stmt vals) = do
      putByte 'B'
      putWord32be (fromIntegral ml)
      putByteString namb >> zero
      putByteString stmtb >> zero
      putWord16be 0 -- all use text
      putWord16be (fromIntegral (length vals))
      putByteString args
      putWord16be 0 -- all use text
    where args = asByteString (runPut (mapM_ putFieldValue vals))
          namb = asByteString nam
          stmtb = asByteString stmt
          ml = strLen namb + strLen stmtb + 12 + strLen args
  
  put (CancelRequest prc key) = do
      putByte 'F'
      putWord32be 16
      putWord32be 80877102
      putWord32be (fromIntegral prc)
      putWord32be (fromIntegral key)
  
  put (ClosePortal x) = dcp 'C' 'P' x
  put (CloseStatement x) = dcp 'C' 'S' x

  put (CopyData x) = undefined
  
  put CopyDone = putByte 'c' >> putWord32be 4
  
  put (CopyFail x) = do
      putByte 'f'
      putWord32be (fromIntegral ml)
      putByteString a >> zero
    where a = asByteString x
          ml = strLen a + 5
  
  put (DescribePortal x) = dcp 'D' 'P' x
  put (DescribeStatement x) = dcp 'D' 'S' x
    
  put _ = undefined

putStringMessage :: String -> Put
putStringMessage s = do
  let sb = asByteString s
  putWord32be ( fromIntegral (strLen sb + 5))
  putByteString sb
  zero

putFieldValue :: Maybe ByteString -> Put
putFieldValue fv = case fv of 
    Nothing -> putWord32be (fromIntegral (-1 :: Int))
    Just b -> putWord32be (fromIntegral (strLen b)) >> putByteString b

putByte :: Char -> Put
putByte = putWord8 . fromIntegral . ord

dcp :: Char -> Char -> String -> Put
dcp a b x = do
  putByte a
  let nam = asByteString x
  putWord32be (fromIntegral (6 + strLen nam))
  putByte b
  putByteString nam
  zero


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
processResponse :: Chan PgMessage -> Socket -> (String,String) -> IO Bool
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
    else do msg <- reallyRead s ( runGet getInt32 (asLazyByteString (strTail more)) - 4 )
            return $ runGet get (asLazyByteString (strCat [more, msg]))

protocolVersion :: Int
protocolVersion = 0x30000

-- | Put a Haskell string, encoding it to UTF-8, and null-terminating it.
hString :: ByteString -> Put
hString s = putByteString s >> zero

-- | Put a Haskell 32-bit integer.
int32 :: Int -> Put
int32 = putWord32be . fromIntegral 

-- | Put zero-byte terminator.
zero :: Put
zero = putWord8 0

getInt32 :: Get Int
getInt32 = fmap fromIntegral (fmap fromIntegral getWord32be :: Get Int32)

getInt16 :: Get Int
getInt16 = fmap fromIntegral (fmap fromIntegral getWord16be :: Get Int16)


-- | Send a block of bytes on a handle, prepending the complete length.
sendBlock :: Binary a => Socket -> a -> IO ()
sendBlock h outp = sendAll h (asByteString (runPut (put outp)))
  where sendAll j msg = do
           n <- sktSend j msg
           if n <= 0 then error "failed to send"
           else let rm = strDrop (toInteger n) msg in unless (strNull rm) (sendAll j rm)

--------------------------------------------------------------------------------
-- Connection

connectToDb :: String -> IO Postgres
connectToDb conns = do
    let [h,p,un,db]=glx conns
        pk = read p :: Int
    pw <- fmap (fromMaybe "") (passwordFromPgpass h pk db un)
    let smp = [("user", un),("database",db)]

    rr <- connectTo (PgConnInfo conns h pk db un pw)
    conn <- newIORef rr
    reconnect conn -- try to connect now -- earlier error message potential
    return conn

reconnect :: Postgres -> IO PostgresR
reconnect conn = do
  prp@(PostgresR px pgr (ps,qs) (pr,qr)) <- readIORef conn
  case pgr of
    EndSession -> do
      killThread ps
      killThread pr
      
      sock <- socket AF_INET Stream defaultProtocol

      let PostgresX z x i = px
      let PgConnInfo _ host port db un pwd = i

      let hints = defaultHints {addrFamily = AF_INET, addrSocketType = Stream}
      addrInfos <- getAddrInfo (Just hints) (Just host) (Just $ show port)
      connq <- catch (sktConnect sock (addrAddress $ head addrInfos) >> return True )
                     (\x -> printMsg (show (x::SomeException)) >> return False)

      p1 <- forkIO $ doUntil $ sendReq qs sock
      p2 <- forkIO $ doUntil $ processResponse qr sock (un,pwd)

      writeChan z (StartUpMessage [("user", un), ("database", db)])
      res <- readChan x
      printMsg ("Connection: "++ show res)
      let wr = PostgresR px res (p1, qs) (p2, qr)
      writeIORef conn wr
      return wr
    _ -> return prp

connectTo :: PgConnInfo -> IO PostgresR -- host port path headers 
connectTo ci@(PgConnInfo _ host port db uid pwd) = do
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
                      ReadyForQuery z -> if (not . null) saccum then return $ OtherResult (reverse saccum)
                                                                else getResults x g
                      -- do
--                        (bs, cc) <- unfoldWhile (getDataRow x)
--                        let cx = case cc of { CommandComplete z -> z; _ -> show cc }
--                         getResults x (ResultSet (Just rd) bs cx : accum)
                      _ -> getResults x (daccum, oaccum, a : saccum)

--------------------------------------------------------------------------------
-- Connection String
keyval :: CharParser () (String,String)
keyval = do
  _ <- spaces
  a <- many1 alphaNum
  _ <- char '=' <|> char ':'
  b <- many1 (noneOf " ")
  _ <- spaces
  traceShow (a,b) $ return (a,b)

dlml :: CharParser () [String]
dlml = do
  z <- many keyval
  let u = unsafePerformIO (getEnv "USER")
      a = lookupWithDefault "localhost" "host" z
      b = lookupWithDefault "5432" "port" z
      c = lookupWithDefault u "user" z
      d = lookupWithDefault "" "dbname" z
  
  return [a,b,c,d]




glx :: String -> [String]
glx x =
  let z = parse dlml "" x
  in case z of 
     Right y -> y
     Left y -> error (show y)

--------------------------------------------------------------------------------
-- pgpass support
xtail :: [a] -> [a]
xtail x = if null x then x else tail x

parsePgpassLine :: String -> (String,Int,String,String,String)
parsePgpassLine a =
  let (h,z) = break (==':') a
      (p,y) = break (==':') (xtail z)
      nm = (reads p :: [(Int, String)])
      p2 = if null nm then 0 else (fst . head) nm
      (d,x) = break (==':') (xtail y)
      (u,v) = break (==':') (xtail x)
      (pw,_zz) = break (==':') (xtail v)
   in (h,p2,d,u,pw)


valine :: String -> Bool
valine a = let z = dropWhile isSpace a
            in not (null z) && (head z /= '#')

passwordFromPgpass :: String -> Int -> String -> String -> IO (Maybe String)
passwordFromPgpass h p dn uid = do
    hm <- getHomeDirectory
    a <- readFile (hm </> ".pgpass")
    let b = map parsePgpassLine (filter valine (lines a))
    let c = filter (pgmatch h p dn uid) b
    return (if null c then Nothing else let (_,_,_,_,r) = head c in Just r)
  where pgmatch k w dx u (k',w',dx',u',_) = k' == k && w' == w && ( dx' == "*" || dx' == dx ) && u' == u

--------------------------------------------------------------------------------

