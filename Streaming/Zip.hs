-- | This module modifies material in Renzo Carbonara\'s <http://hackage.haskell.org/package/pipes-zlib pipes-zlib> package.   

module Streaming.Zip (
    -- * Streams
      decompress
    , decompress'
    , decompressAll
    , compress
    , gunzip
    , gunzip'
    , gzip 

    -- * Compression levels
    , CompressionLevel
    , defaultCompression
    , noCompression
    , bestSpeed
    , bestCompression
    , compressionLevel

    -- * Window size
    -- $ccz-re-export
    , Z.defaultWindowBits
    , windowBits
    ) where 
  
import           Data.Streaming.Zlib       as Z
import           Control.Exception         (throwIO)
import           Control.Monad             (unless)
import qualified Data.ByteString           as B
import Data.ByteString.Streaming 
import Streaming
import qualified Data.ByteString.Streaming.Internal as I 
import Data.ByteString.Streaming.Internal (ByteString (..)) 



--------------------------------------------------------------------------------

-- | Decompress a streaming bytestring. 'Z.WindowBits' is from "Codec.Compression.Zlib" 
--
-- @
-- 'decompress' 'defaultWindowBits' :: 'MonadIO' m => 'ByteString' m r -> 'ByteString' m r
-- @

decompress
  :: MonadIO m
  => Z.WindowBits
  -> ByteString m r -- ^ Compressed stream
  -> ByteString m r -- ^ Decompressed stream
decompress wbits p0 = do
    inf <- liftIO $ Z.initInflate wbits
    r <- for p0 $ \bs -> do
       popper <- liftIO (Z.feedInflate inf bs)
       fromPopper popper
    bs <- liftIO $ Z.finishInflate inf
    unless (B.null bs) (chunk bs)
    return r
{-# INLINABLE decompress #-}

-- | Decompress a zipped byte stream, returning any leftover input
-- that follows the compressed material.
decompress'
  :: MonadIO m
  => Z.WindowBits
  -> ByteString m r -- ^ Compressed byte stream
  -> ByteString m (Either (ByteString m r) r)
     -- ^ Decompressed byte stream, ending with either leftovers or a result
decompress' wbits p0 = go p0 =<< liftIO (Z.initInflate wbits)
  where
    flush inf = do
      bs <- liftIO $ Z.flushInflate inf
      unless (B.null bs) (chunk bs)
    go p inf = do
      res <- lift (nextChunk p)
      case res of
         Left r -> return $ Right r
         Right (bs, p') -> do
            fromPopper =<< liftIO (Z.feedInflate inf bs)
            flush inf
            leftover <- liftIO $ Z.getUnusedInflate inf
            if B.null leftover
               then go p' inf
               else return $ Left (chunk leftover >> p')
{-# INLINABLE decompress' #-}

-- | Keep decompressing a compressed bytestream until exhausted.
decompressAll :: MonadIO m => Z.WindowBits -> ByteString m r -> ByteString m r
decompressAll w bs = decompress' w bs >>= go
  where
    go (Left bs) = decompress' w bs >>= go
    go (Right r) = return r
{-# INLINABLE decompressAll #-}

-- | Compress a byte stream.
--
-- See the "Codec.Compression.Zlib" module for details about
-- 'Z.CompressionLevel' and 'Z.WindowBits'.

-- @
-- 'compress' 'defaultCompression' 'defaultWindowBits' :: 'MonadIO' m => 'ByteString' m r -> 'ByteString' m r
-- @
-- 
compress
  :: MonadIO m
  => CompressionLevel
  -> Z.WindowBits
  -> ByteString m r -- ^ Decompressed stream
  -> ByteString m r -- ^ Compressed stream
compress (CompressionLevel clevel) wbits p0 = do
    def <- liftIO $ Z.initDeflate clevel wbits
    let loop bs = case bs of 
          I.Chunk c rest -> do
            popper <- liftIO (Z.feedDeflate def c)
            fromPopper popper
            loop rest
          I.Go m -> I.Go (liftM loop m)
          I.Empty r -> return r
    r <- loop p0
    fromPopper $ Z.finishDeflate def
    return r
{-# INLINABLE compress #-}

--------------------------------------------------------------------------------

-- $ccz-re-export
--
-- The following are re-exported from "Codec.Compression.Zlib" for your
-- convenience.

--------------------------------------------------------------------------------
-- Compression Levels

-- | How hard should we try to compress?
newtype CompressionLevel = CompressionLevel Int
                         deriving (Show, Read, Eq, Ord)

defaultCompression, noCompression, bestSpeed, bestCompression :: CompressionLevel
defaultCompression = CompressionLevel (-1)
noCompression      = CompressionLevel 0
bestSpeed          = CompressionLevel 1
bestCompression    = CompressionLevel 9

-- | A specific compression level between 0 and 9.
compressionLevel :: Int -> CompressionLevel
compressionLevel n
  | n >= 0 && n <= 9 = CompressionLevel n
  | otherwise        = error "CompressionLevel must be in the range 0..9"

windowBits :: Int -> WindowBits
windowBits = WindowBits

-- | Decompress a gzipped byte stream.

gunzip
  :: MonadIO m
  => ByteString m r -- ^ Compressed stream
  -> ByteString m r -- ^ Decompressed stream
gunzip = decompress gzWindowBits
{-# INLINABLE gunzip #-}

-- | Decompress a gzipped byte stream, returning any leftover input
-- that follows the compressed stream.
gunzip'
  :: MonadIO m
  => ByteString m r -- ^ Compressed byte stream
  -> ByteString m (Either (ByteString m r) r)
     -- ^ Decompressed bytes stream, returning either a 'ByteString' of 
      -- the leftover input or the return value from the input 'ByteString'.
gunzip' = decompress' gzWindowBits
{-# INLINE gunzip' #-}


-- | Compress a byte stream in the gzip format.

gzip
  :: MonadIO m
  => CompressionLevel
  -> ByteString m r -- ^ Decompressed stream
  -> ByteString m r -- ^ Compressed stream
gzip clevel = compress clevel gzWindowBits
{-# INLINE gzip #-}

gzWindowBits :: Z.WindowBits
gzWindowBits = Z.WindowBits 31


--------------------------------------------------------------------------------
-- Internal stuff


for bs0 op = loop bs0 where
  loop bs = case bs of 
    I.Chunk c rest -> op c >> loop rest
    I.Go m -> I.Go (liftM loop m)
    I.Empty r -> return r
{-# INLINABLE for #-}

-- | Produce values from the given 'Z.Popper' until exhausted.
fromPopper :: MonadIO m
           => Z.Popper
           -> ByteString m ()
fromPopper pop = loop
  where
    loop = do 
      mbs <- liftIO pop
      case mbs of
          PRDone     -> I.Empty ()
          PRError e  -> I.Go (liftIO (throwIO e))
          PRNext bs  -> I.Chunk bs loop
{-# INLINABLE fromPopper #-}

