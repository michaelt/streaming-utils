{-# LANGUAGE OverloadedStrings #-}
-- | This module replicates `pipes-http` as closely as will type-check, adding a
--   conduit-like @http@ in @ResourceT@ and a primitive @simpleHTTP@ that emits
--   a streaming bytestring rather than a lazy one. 
--
--  
--   Here is an example GET request that streams the response body to standard output:
--
-- > import qualified Data.ByteString.Streaming as Q
-- > import Data.ByteString.Streaming.HTTP
-- >
-- > main = do
-- >   req <- parseRequest "https://www.example.com"
-- >   m <- newManager tlsManagerSettings 
-- >   withHTTP req m $ \resp -> Q.stdout (responseBody resp) 
-- > 
--
--   Here is an example POST request that also streams the request body from
--   standard input:
--
-- > {-#LANGUAGE OverloadedStrings #-}
-- > import qualified Data.ByteString.Streaming as Q
-- > import Data.ByteString.Streaming.HTTP
-- > 
-- > main = do
-- >    req <- parseRequest "https://httpbin.org/post"
-- >    let req' = req
-- >            { method = "POST"
-- >            , requestBody = stream Q.stdin
-- >            }
-- >    m <- newManager tlsManagerSettings
-- >    withHTTP req' m $ \resp -> Q.stdout (responseBody resp)
--
-- Here is the GET request modified to use @http@ and write to a file. @runResourceT@
-- manages the file handle and the interaction.
--
-- > import qualified Data.ByteString.Streaming as Q
-- > import Data.ByteString.Streaming.HTTP
-- >
-- > main = do
-- >   req <- parseUrlThrow "https://www.example.com"
-- >   m <- newManager tlsManagerSettings 
-- >   runResourceT $ do
-- >      resp <- http request manager 
-- >      Q.writeFile "example.html" (responseBody resp) 
--
-- 
--   @simpleHTTP@ can be used in @ghci@ like so:
--
--  > ghci> runResourceT $ Q.stdout $ Q.take 137 $ simpleHTTP "http://lpaste.net/raw/13"
--  > -- Adaptation and extension of a parser for data definitions given in
--  > -- appendix of G. Huttons's paper - Monadic Parser Combinators.
--  > --

-- For non-streaming request bodies, study the 'RequestBody' type, which also
-- accepts strict \/ lazy bytestrings or builders.


module Data.ByteString.Streaming.HTTP (

    -- * Streaming Interface
    withHTTP
    , http
    , streamN
    , stream
    
    -- * ghci testing
    , simpleHTTP

    -- * re-exports
    , module Network.HTTP.Client
    , module Network.HTTP.Client.TLS
    , ResourceT (..)
    , MonadResource (..)
    , runResourceT
    ) where

import Control.Monad (unless)
import qualified Data.ByteString as B
import Data.Int (Int64)
import Data.IORef (newIORef, readIORef, writeIORef)
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Data.ByteString.Streaming
import Data.ByteString.Streaming.Internal
import Control.Monad.Trans
import Control.Monad.Trans.Resource
import qualified Data.ByteString.Streaming.Char8 as Q
{- $httpclient
    This module is a thin @streaming-bytestring@ wrapper around the @http-client@ and
    @http-client-tls@ libraries.

    Read the documentation in the "Network.HTTP.Client" module of the
    @http-client@ library to learn about how to:

    * manage connections using connection pooling,

    * use more advanced request\/response features,

    * handle exceptions, and:
    
    * manage cookies.

    @http-client-tls@ provides support for TLS connections (i.e. HTTPS).
-}

-- | Send an HTTP 'Request' and wait for an HTTP 'Response'
withHTTP
    :: Request
    -- ^
    -> Manager
    -- ^
    -> (Response (ByteString IO ()) -> IO a)
    -- ^ Handler for response
    -> IO a
withHTTP r m k = withResponse r m k'
  where
    k' resp = do
        let p = (from . brRead . responseBody) resp
        k (resp { responseBody = p})
{-# INLINABLE withHTTP #-}

-- | Create a 'RequestBody' from a content length and an effectful 'ByteString'
streamN :: Int64 -> ByteString IO () -> RequestBody
streamN n p = RequestBodyStream n (to p)
{-# INLINABLE streamN #-}

{-| Create a 'RequestBody' from an effectful 'ByteString'

    'stream' is more flexible than 'streamN', but requires the server to support
    chunked transfer encoding.
-}
stream :: ByteString IO () -> RequestBody
stream p = RequestBodyStreamChunked (to p)
{-# INLINABLE stream #-}

to :: ByteString IO () -> (IO B.ByteString -> IO ()) -> IO ()
to p0 k = do
    ioref <- newIORef p0
    let readAction :: IO B.ByteString
        readAction = do
            p <- readIORef ioref
            case p of
                Empty   ()      -> do
                    writeIORef ioref (return ())
                    return B.empty
                Go m -> do 
                  p' <- m
                  writeIORef ioref p'
                  readAction
                Chunk bs p' -> do
                    writeIORef ioref p'
                    return bs
    k readAction 

-- from :: IO B.ByteString -> ByteString IO ()
from io = go
  where
    go = do
        bs <- lift io
        unless (B.null bs) $ do
            chunk bs
            go 
            
{-| This is a quick method - oleg would call it \'unprofessional\' - to bring a web page in view.
    It sparks its own internal manager and closes itself. Thus something like this makes sense

>>> runResourceT $ Q.putStrLn $ simpleHttp "http://lpaste.net/raw/12"
chunk _ [] = []
chunk n xs = let h = take n xs in h : (chunk n (drop n xs))
            
    but if you try something like

>>> rest <- runResourceT $ Q.putStrLn $ Q.splitAt 40 $ simpleHTTP "http://lpaste.net/raw/146532"
import Data.ByteString.Streaming.HTTP 

    it will just be good luck if with 
            
>>> runResourceT $ Q.putStrLn rest
            
    you get the rest of the file: 
            
> import qualified Data.ByteString.Streaming.Char8 as Q
> main = runResourceT $ Q.putStrLn $ simpleHTTP "http://lpaste.net/raw/146532"
 
    rather than 
            
> *** Exception: <socket: 13>: hGetBuf: illegal operation (handle is closed)
            
    Since, of course, the handle was already closed by the first use of @runResourceT@.
    The same applies of course to the more hygienic 'withHTTP' above, 
    which permits one to extract an @IO (ByteString IO r)@, by using @splitAt@ or
    the like. 
            
    The reaction of some streaming-io libraries was simply to forbid
    operations like @splitAt@. That this paternalism was not viewed
    as simply outrageous is a consequence of the opacity of the
    older iteratee-io libraries. It is /obvious/ that I can no more run an
    effectful bytestring after I have made its effects impossible by
    using @runResourceT@ (which basically means @closeEverythingDown@). 
    I might as well try to run it after tossing my machine into the flames. 
    Similarly, it is obvious that I cannot read from a handle after I have 
    applied @hClose@; there is simply no difference between the two cases.
-}
simpleHTTP :: MonadResource m => String -> ByteString m ()
simpleHTTP url = do
    man <- liftIO (newManager tlsManagerSettings)
    req <- liftIO (parseUrlThrow url)
    bracketByteString 
       (responseOpen req man) 
       responseClose 
       ( from . liftIO . responseBody)


http :: MonadResource m
      => Request
      -> Manager
      -> m (Response (ByteString m ()))
http req man = do
     (key, res) <- allocate (responseOpen req man) responseClose
     return res {responseBody = from (liftIO (responseBody res))}
            

