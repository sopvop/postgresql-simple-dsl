{-# LANGUAGE OverloadedStrings #-}
module Database.PostgreSQL.Simple.Dsl.Escaping
       ( escapeByteA
       , escapeByteAFast
       , escapeByteAHex
       , escapeString
       , escapeIdentifier
       , escapeAction
       ) where

import           Data.Bits
import           Data.ByteString                    (ByteString)
import           Data.ByteString.Builder            (Builder, byteString, byteStringHex,
                                                     word8)
import           Data.ByteString.Builder.Prim       (condB, liftFixedToBounded,
                                                     primMapByteStringBounded, (>$<),
                                                     (>*<))
import qualified Data.ByteString.Builder.Prim       as P
import           Data.Monoid
import           Data.Word                          (Word8)

import           Database.PostgreSQL.Simple.ToField


toOctets :: Word8 -> (Word8, (Word8, Word8))
toOctets x = ((x `shiftR` 6) + 48, ((shiftR x 3 .&. 0x7) + 48, (x .&. 0x7) + 48))
{-# INLINE toOctets #-}

octetF :: P.FixedPrim Word8
octetF = ((,) 0x5c . (,) (0x5c) . toOctets)
         >$< P.word8 >*< P.word8 >*< P.word8 >*< P.word8 >*< P.word8
{-# INLINE octetF #-}

escapeByteA :: ByteString -> Builder -- for 9.0+
escapeByteA bs = " E'" <> primMapByteStringBounded escape bs <> word8 0x27
  where
    escape = condB (\c -> c < 32 || c > 126 || c == 0x5c || c == 0x27)
                   (liftFixedToBounded octetF)
                   (liftFixedToBounded P.word8)

-- | Result is longer by 1.5 than escapeByteA, but should be less allocations
escapeByteAFast :: ByteString -> Builder
escapeByteAFast bs = " E'" <> P.primMapByteStringFixed octetF bs <> word8 0x27

-- | Result is much smaller, requires pg 9.0+
escapeByteAHex :: ByteString -> Builder
escapeByteAHex bs = "'\\x" <> byteStringHex bs <> word8 0x27


escapeString :: ByteString -> Builder
escapeString bs = byteString " E'" <> primMapByteStringBounded escape bs <> word8 0x27
  where
    escape :: P.BoundedPrim Word8
    escape =
      condB (== 0x5c) (fixed2 (0x5c, 0x5c)) $
      condB (== 0x27) (fixed2 (0x27, 0x27)) $
      liftFixedToBounded P.word8
    {-# INLINE fixed2 #-}
    fixed2 x = liftFixedToBounded $ const x >$< P.word8 >*< P.word8

escapeIdentifier :: ByteString -> Builder
escapeIdentifier bs = word8 0x22 <> primMapByteStringBounded escape bs <> word8 0x22
  where
    escape :: P.BoundedPrim Word8
    escape =
      condB (== 0x22) (fixed2 (0x22, 0x22)) $
      liftFixedToBounded P.word8
    {-# INLINE fixed2 #-}
    fixed2 x = liftFixedToBounded $ const x >$< P.word8 >*< P.word8

escapeAction :: Action -> Builder
escapeAction (Plain a ) = a
escapeAction (Escape t) = escapeString t
escapeAction (EscapeByteA t) = escapeByteA t
escapeAction (EscapeIdentifier t) = escapeIdentifier t
escapeAction (Many xs) = mconcat (map escapeAction xs)
