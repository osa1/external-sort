{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ExternalSort where
{-
  ( sortExternal
  , Get
  , Put
  ) where
  -}

import Control.Monad (forM, forM_)
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.List (sort)
import System.FilePath ((</>))
import System.IO

type Get handle a = handle -> IO (Maybe a, handle)
type Put handle a = handle -> a -> IO ()

type GetFile a = Get Handle a
type PutFile a = Put Handle a

-- | State to incrementally read ByteString from a file handle and parse using
-- `Data.Binary.Get`.
data BinaryHandle = BinaryHandle
    { _bhHandle    :: !(Maybe Handle)
        -- ^ Not available after reaching EOF.
    , _bhLeftovers :: ![BS.ByteString]
        -- ^ Leftover from previous get.
    , _bhReadSize  :: !Int
        -- ^ How much to read from file to parse.
    }

binaryGet :: forall a . Show a => B.Get a -> Get BinaryHandle a
binaryGet get =
    go (B.runGetIncremental get)
  where
    go :: B.Decoder a -> BinaryHandle -> IO (Maybe a, BinaryHandle)

    go (B.Fail _ _ reason) bh
      = putStrLn reason >> return (Nothing, bh)

    go (B.Partial cont) bh@(BinaryHandle mb_file leftovers read_size)
      = case leftovers of
          l : ls ->
            go (cont (Just l)) bh{ _bhLeftovers = ls }
          [] ->
            case mb_file of
              Nothing ->
                -- EOF reached
                go (cont Nothing) bh
              Just file -> do
                bs <- BS.hGetSome file read_size
                let bh'
                      | BS.length bs < read_size
                      = bh{ _bhHandle = Nothing }
                      | otherwise
                      = bh
                go (cont (if BS.null bs then Nothing else Just bs)) bh'

    go (B.Done bs _ a) bh
      = do -- putStrLn ("Read: " ++ show a ++ " Leftover length: " ++ show (BS.length bs))
           return (Just a, bh{ _bhLeftovers = bs : _bhLeftovers bh })

binaryPut :: (a -> B.Put) -> Put Handle a
binaryPut put h a = LBS.hPut h (B.runPut (put a))

sortExternal
    :: Ord a
    => Int      -- ^ How many elements in a chunk?
    -> FilePath -- ^ Root directory for temporary files
    -> FilePath -- ^ File to sort
    -> Get get_handle a -- ^ How to read one element from a file
    -> Put put_handle a -- ^ How to write one element to a file
    -> (FilePath -> IO get_handle) -- ^ How to open a file for reading
    -> (FilePath -> IO put_handle) -- ^ How to open a file for writing
    -> (get_handle -> IO ()) -- ^ How to close get handle
    -> (put_handle -> IO ()) -- ^ How to close put handle
    -> FilePath -- ^ Where to put the sorted file
    -> IO ()

sortExternal chunk_size tmp_dir file_in get put mk_get mk_put close_get close_put file_out = do
    -- Generate sorted chunk files
    n_chunks <- do
      h <- mk_get file_in
      ret <- generateChunks h chunk_size tmp_dir get put mk_put close_put
      close_get h
      return ret

    -- Read chunk_size/(n_chunks + 1) sized chunks from each chunk, do
    -- n_chunks-way merge sort, write result to the output file.
    let
      sort_chunk_size = max (chunk_size `div` (n_chunks + 1)) 1
      sort_steps = ceiling (fromIntegral chunk_size / fromIntegral (n_chunks + 1) :: Double) :: Int

    -- TODO: These handles will leak on exception
    -- TODO: close handles
    hs <- forM [0..n_chunks-1] (\n_chunk -> mk_get (tmp_dir </> show n_chunk))
    h_out <- mk_put file_out

    sort_loop sort_steps sort_chunk_size hs h_out
  where
    sort_loop 0 _ hs h_out = do
      forM_ hs close_get
      close_put h_out

    sort_loop n sort_chunk_size hs h_out = do
      -- TODO: Drop files that return Nothing for the next iteration
      (sorted_chunks, hs') <-
        fmap unzip $
        forM hs (\h -> fmap (\(is, h', _) -> (is, h'))
                            (readChunk get h sort_chunk_size))

      forM_ (merge sorted_chunks) (put h_out)

      sort_loop (n - 1) sort_chunk_size hs' h_out

-- | Simple N-way merge
merge :: Ord a => [[a]] -> [a]
merge = foldr merge2 []

-- | 2-way merge
merge2 :: Ord a => [a] -> [a] -> [a]
merge2 l1@(i1 : is1) l2@(i2 : is2)
  | i1 < i2   = i1 : merge2 is1 l2
  | otherwise = i2 : merge2 l1 is2
merge2 l1 [] = l1
merge2 [] l2 = l2

-- | Split the file into `chunk_size`-sized sorted chunks. Put chunks in the `tmp_dir`.
generateChunks
    :: forall get_handle put_handle a . Ord a
    => get_handle
    -> Int
    -> FilePath
    -> Get get_handle a
    -> Put put_handle a
    -> (FilePath -> IO put_handle)
    -> (put_handle -> IO ())
    -> IO Int
generateChunks h_in0 chunk_size tmp_dir get put mk_put close_put =
    go 0 h_in0
  where
    go
      :: Int -- ^ Number of the current chunk file
      -> get_handle
      -> IO Int
    go n_chunk h_in = do
      (chunk_unsorted, h_in', eof) <- readChunk get h_in chunk_size
      h_out <- mk_put (tmp_dir </> show n_chunk)
      forM_ (sort chunk_unsorted) (put h_out)
      close_put h_out
      if eof then return n_chunk else go (n_chunk + 1) h_in'

-- | Returns elements in the order they appear in the file and whether got
-- `Nothing` from `get`.
readChunk
    :: Get get_handle a
    -> get_handle -- ^ File to read the chunk from
    -> Int        -- ^ Size of the chunk
    -> IO ([a], get_handle, Bool)
readChunk get h0 chunk_size =
    go chunk_size h0
  where
    go 0 h =
      return ([], h, False)
    go n h =
      get h >>= \case
        (Nothing, h') -> return ([], h', True)
        (Just i, h')  -> do
          (is, h'', eof) <- go (n - 1) h'
          return (i : is, h'', eof)
