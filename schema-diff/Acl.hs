{-# LANGUAGE FlexibleInstances, OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

module Acl (
  cvtacl,
  showAclDiffs,
  Acl
)
where

import PostgreSQL
import Preface.R0ml
import Util
import Data.List (sort, (\\))
-- import Data.List
import Debug.Trace
import Text.ParserCombinators.Parsec hiding ((<|>))

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
  
dlml :: Parser [Text]
dlml = sepBy qsts (char ',')

glx :: Text -> [Text]
glx x =
  let z = parse dlml "" ((tail . init) (asString x))
  in case z of 
     Right y -> y
     Left y -> error (show y)
  
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
toAcl x = let  (p,q) = (strBreak ('/'==) x)
               (a,b) = (strBreak ('='==) p)
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
