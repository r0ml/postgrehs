
module Util (
  module Console,
  module Util,
)

where

import PostgreSQL
import Preface
import Console

gs :: PgResult -> Text
gs = undefined

gi :: PgResult -> Int
gi = undefined

gb :: PgResult -> Bool
gb = undefined


{-
gs :: SqlValue -> String
gs y@(SqlByteString x) = fromSql y
gs SqlNull = ""

gb :: SqlValue -> Bool
gb y@(SqlBool x) = fromSql y

gi :: SqlValue -> Int
gi y@(SqlInt32 x) = fromSql y
-}

data Comparison a = Equal a | LeftOnly a | RightOnly a | Unequal a a

sok :: Text
sok = strConcat [ setColor dullGreen,  [charCheck] ,  " "]
nok :: Text
nok = strConcat [setColor dullRed, setAttr bold, [charNotEquals], " "]

trim :: Text -> Text
trim [] = []
trim x@(a:y) = if (isSpace a) then trim y else x

compareIgnoringWhiteSpace :: Text -> Text -> Bool
compareIgnoringWhiteSpace x y = ciws (trim x) (trim y)
  where ciws x y =
          let a = strHead x
              b = strHead y
              p = strTail x
              q = strTail y
           in if (isSpace a && isSpace b) then ciws (stripStart p) (stripStart q) else
              if (a == b) then ciws p q else False
        ciws x [] = strNull (stripStart x)
        ciws [] y = strNull (stripStart y)

count x a = foldl (flip ((+) . fromEnum . x)) 0 a
dcount x y = foldl (\(a,b) z -> if (x z) then (a+1,b) else (a,b+1)) (0,0) y 

iseq x = case x of { Equal _ -> True; _ -> False }

class Ord a => Comparable a where
  -- doDbCompare :: [a] -> [a] -> [Comparison a]
  dbCompare :: [a] -> [a] -> [Comparison a]
  dbCompare x@(a:r) [] = map LeftOnly x
  dbCompare [] y@(a:r) = map RightOnly y
  dbCompare [] [] = []
  dbCompare x@(a:r) y@(b:s) = case compare a b of
      EQ -> objCmp a b : dbCompare r s
      LT -> LeftOnly a : dbCompare r y 
      GT -> RightOnly b : dbCompare x s

  objCmp :: a -> a -> Comparison a
