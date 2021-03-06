{-# LANGUAGE FlexibleInstances, OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Acl (
  cvtacl,
  showAclDiffs,
  Acl
)
where

import PostgreSQL
import Preface
import Util

-- import Text.ParserCombinators.Parsec hiding ((<|>))
{-
dquote = char '"' <?> "double quote"
quoted_char = do
    _ <- char '\\'
    r <- char '"'
    return r
  <?> "quoted_char"
                 
qtext = fmap asText (many( quoted_char <|> noneOf "\""))
quoted_string = do 
    _ <- dquote
    r <- qtext
    _ <- dquote
    return r
  <?> "quoted string"

qsts :: Parser Text
qsts = do
  r <- many (quoted_string <|> fmap asText ( many1 (noneOf ",")))
  return $ strConcat r
  -}

dlml :: Text -> [Text]
dlml z = if strNull z then []
         else if strHead z == '"'
              then let (p,q) = popStr z
                    in p : dlml (strTail q)
              else let zx = strElemIndex ',' z
                    in case zx of Nothing -> [z]
                                  Just n -> strTake n z : dlml (strDrop (1+n) z)
  where popStr x = popSt (strTail x) where
           popSt :: Text -> (Text, Text)
           popSt x = if strNull x then (strEmpty, strEmpty)
                       else let b = strHead x
                                x2 = strTail x
                             in case b of 
                                  '"' -> (strEmpty, x2) 
                                  '\\' -> let b2 = strHead x2
                                              x3 = strTail x2
                                           in let (j,k) = popSt x3 in (strCons b2 j, k)
                                  _ -> let (j,k) = popSt x2 in (strCons b j, k)


glx :: Text -> [Text]
glx x = dlml $ (strTail . strInit) x
  
instance PgValue [Acl] where
  fromPg = maybe [] (cvtacl . asText)

cvtacl :: Text -> [Acl]
cvtacl x = if (strNull x) then [] else sort $ map toAcl (glx x)

instance Comparable Acl where
  objCmp a b =
    if (grantee a == grantee b && privileges a == privileges b) then Equal a 
    else Unequal a b

data Acl = Acl { grantee :: Text, privileges :: [Char], grantor :: Text }

instance Show Acl where
  show a = concat [asString (grantee a), " has ", intercalate "," $ map privName (privileges a) ]

privName x = case x of { ('a') -> "INSERT"; ('r') -> "SELECT"; ('w')->"UPDATE"; ('d')->"DELETE"
                        ; ('D') -> "TRUNCATE"; ('x') -> "REFERENCES"; ('t') -> "TRIGGER"; ('X')->"EXECUTE"
                        ; ('U') -> "USAGE"; ('C') -> "CREATE"; ('T')->"CREATE TEMP"; ('c')->"CONNECT"; _ ->"? "++[x] }

toAcl :: Text -> Acl
toAcl x = let  (p,q) = (strBrk ('/'==) x)
               (a,b) = (strBrk ('='==) p)
          in Acl (if (strNull a) then "public" else if (strHead a == '"') then (read (asString a) :: Text) else asText a ) (asString (strTail b)) (strTail q)

instance Ord Acl where
  compare a b = compare (grantee a) (grantee b)

instance Eq Acl where
  (==) a b = grantee a == grantee b && privileges a == privileges b
  
instance Show (Comparison Acl) where
  show (Equal x) = ""
  show (LeftOnly a) = concat [asString azure, stringleton charLeftArrow," ", show a, asString treset]
  show (RightOnly a) = concat [asString peach, stringleton charRightArrow, " ", show a,  asString treset]
  show (Unequal a b) = let p = map privName (privileges a \\ privileges b)
                           q = map privName (privileges b \\ privileges a)
                       in concat [asString nok, asString $ grantee a, if (null p) then "" else concat [" also has ", asString azure , intercalate "," p],
                                             if (null q) then "" else concat ["; lacks ", asString peach, intercalate "," q],
                                  asString treset ]

showAclDiffs a b = 
  let dc = dbCompare a b
      ddc = filter (\l -> case l of { Equal _ -> False; _ -> True }) dc
  in if ( a /=  b) 
     then concat [ asString $ setAttr bold, "\n acls: ", asString treset, "\n  ", intercalate "\n  " $ map show ddc] 
     else ""
