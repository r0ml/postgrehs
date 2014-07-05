{-# LANGUAGE OverloadedStrings #-}

module MD5 (md5, md5File, stringMD5) where

import Preface
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B (toForeignPtr)

{-
import qualified Data.ByteString as B (concat, concatMap, index, singleton, length, pack, take, drop, cons, replicate, hGetContents)
import Data.ByteString.Internal (ByteString, toForeignPtr)
import Data.Bits ((.&.), shiftR, (.|.), complement, xor, rotateL)
import Data.List (foldl')
import Data.Word (Word32, Word64)

import Foreign.Marshal.Array (peekArray)
import Foreign.Ptr (Ptr, plusPtr, castPtr)
import Foreign.ForeignPtr (withForeignPtr)
import System.IO (openFile, IOMode(ReadMode))
import System.IO.Unsafe (unsafePerformIO)
-}

data MD5State = MD5State Word32 Word32 Word32 Word32 deriving Show
type Digest = B.ByteString

md5 :: B.ByteString -> Digest
md5 arg = (md5Finalize . foldl' md5Update md5InitialState . blocks) arg
      where md5InitialState = MD5State 0x67452301 0xEFCDAB89 0x98BADCFE 0x10325476
            md5Finalize (MD5State a b c d) = B.pack $ concatMap intToBytes [a,b,c,d]
            intToBytes n = map (fromIntegral . (.&. 0xff) . shiftR n) (take 4 [0,8..])
            blocks bs = if B.length bs >= bsz then B.take bsz bs : blocks ( B.drop bsz bs) 
                        else let lzp = (bsz - 9) - B.length bs
                                 lzpx = if lzp < 0 then lzp+bsz else lzp
                                 lm = B.concat [bs, B.singleton 0x80, B.replicate lzpx 0, longToBytes (8 * olen)]
  	                         in if lzp < 0 then [ B.take bsz lm, B.drop bsz lm] else [lm]
            bsz = 64
            olen = fromIntegral (B.length arg)
            longToBytes :: Word64 -> B.ByteString
            longToBytes n =  B.pack $ map (fromIntegral . (.&. 0xff) . shiftR n) (take 8 [0,8..])

-- takes 64 byte ByteString at a time.
md5Update :: MD5State -> B.ByteString -> MD5State
md5Update (MD5State a b c d) w =
    let ws = cycle (wordsFromBytes w)
 
        m1 = [ 0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501
             , 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821 ]

        m2 = [ 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8
             , 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a ]

        m3 = [ 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70
	         , 0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665 ]

        m4 = [ 0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1
             , 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391 ]

        (z1,z2,z3,z4) = foldl ch2 (a,b,c,d) [
           (\x y z -> z `xor` (x .&. (y `xor` z)) , ws,                     [ 7, 12, 17, 22], m1),
           (\x y z -> y `xor` (z .&. (x `xor` y)) , everynth 4 (tail ws),   [ 5,  9, 14, 20], m2),
           (\x y z -> x `xor` y `xor` z ,           everynth 2 (drop 5 ws), [ 4, 11, 16, 23], m3),
           (\x y z -> y `xor` (x .|. complement z), everynth 6 ws         , [ 6, 10, 15, 21], m4) ]

    in MD5State (z1+a) (z2+b) (z3+c) (z4+d)
    where
      mch fn h i j k x s ac = i + rotateL (fn i j k + (x + ac + h)) s
      ch1 fn (h,i,j,k) ws rs ms = let r = mch fn h i j k (head ws) (head rs) (head ms)
                 in r : (if null (tail ms) then [] else ch1 fn (k,r,i,j) (tail ws) (tail rs) (tail ms) )
      ch2 e (f,w1,r,m) = let [h,i,j,k] = drop 12 (ch1 f e w1 (cycle r) m) in (h,k,j,i)
      everynth n ys = head ys : everynth n (drop (n+1) ys )
      wordsFromBytes bs = unsafePerformIO $ withForeignPtr ptr $ \p -> peekArray (len `div` 4) (castPtr (p `plusPtr` off) :: Ptr Word32) where (ptr, off, len) = B.toForeignPtr bs

stringMD5 :: B.ByteString -> B.ByteString
stringMD5 = B.concatMap (shex . fromEnum)
  where shex n = let (a,b) = divMod n 16 in B.cons (B.index chars a) (B.singleton (B.index chars b))
        chars = "0123456789abcdef" :: B.ByteString

md5File :: String -> IO ()
md5File f = openFile f ReadMode >>= B.hGetContents >>= print . stringMD5 . md5
