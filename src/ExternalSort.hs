{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ExternalSort where
{-
  ( sortExternal
  , Get
  , Put
  ) where
  -}

import Control.Monad
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.List (sort, minimumBy)
import System.FilePath ((</>))
import System.IO
import Data.Ord
import qualified Data.Vector.Mutable as MV
import qualified Data.Vector as V

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
    let sort_chunk_size = max (chunk_size `div` (n_chunks + 1)) 1

    -- TODO: These handles will leak on exception
    -- TODO: close handles
    hs <- forM [0..n_chunks-1] (\n_chunk -> mk_get (tmp_dir </> show n_chunk))
    h_out <- mk_put file_out

    merge' (zip (repeat get) hs) close_get sort_chunk_size (put, h_out)
    close_put h_out


--------------------------------------------------------------------------------
-- N-way merge. Load N chunks into memory, merge pair-wise. When a list is
-- completely merged, load more elements. If there are no more elements for that
-- list drop it.

data Chunk get_handle a = Chunk
    { _chunkElems  :: [a]
        -- ^ Elements of the list in memory.
    , _chunkHandle :: get_handle
        -- ^ Handle to read more elements.
    , _chunkEOF    :: Bool
        -- ^ Reached eof?
    , _chunkGet    :: Get get_handle a
        -- ^ Use to get more elements.
    }

peekML :: Int -> Chunk get_handle a -> IO (Maybe a, Chunk get_handle a)
peekML chunk_size (Chunk lst handle eof get)
  | h : _ <- lst
  = return (Just h, Chunk lst handle eof get)

  | not eof
  = do (lst', handle', eof') <- readChunk get handle chunk_size
       if null lst'
         then return (Nothing, Chunk [] handle eof' get)
         else peekML chunk_size (Chunk lst' handle' eof' get)

  | otherwise
  = return (Nothing, Chunk [] handle True get)

unconsML :: Chunk get_handle a -> (a, Chunk get_handle a)
unconsML ml =
    case _chunkElems ml of
      [] -> error "unconsML: Empty list. Make sure you call peekML first."
      h : t -> (h, ml{ _chunkElems = t })

consML :: a -> Chunk get_handle a -> Chunk get_handle a
consML a l = l{ _chunkElems = a : _chunkElems l }

merge'
    :: forall get_handle put_handle a . Ord a
    => [(Get get_handle a, get_handle)]
    -> (get_handle -> IO ())
    -> Int -- ^ Chunk size
    -> (Put put_handle a, put_handle)
    -> IO ()

merge' ins close_in chunk_size0 (put, put_handle) = do
    -- A vector of lists to merge. Consumed lists are replaced with `Nothing`
    -- and moved to the end of the list.
    mls <- V.thaw (V.fromList (map mk_ml ins))

    go chunk_size0 mls
  where
    mk_ml (get, get_handle) = Chunk [] get_handle False get

    go :: Int -- ^ Chunk size. Update as lists consumed.
       -> MV.IOVector (Chunk get_handle a)
              -- ^ Lists to merge
       -> IO ()

    go _ mls
      | MV.null mls = return ()

    go chunk_size mls = do

      putStrLn ("Looping. Chunk size: " ++ show chunk_size)

      (mls', heads) <- read_mins chunk_size mls 0 []

      unless (null heads) $ do
        let (min_idx, min_) = minimumBy (comparing snd) heads

        min_l <- MV.read mls min_idx
        MV.write mls min_idx (snd (unconsML min_l))

        put put_handle min_

        go (max {- (chunk_size0 `div` MV.length mls') -} chunk_size0 1) mls'

    read_mins
      :: Int
      -> MV.IOVector (Chunk get_handle a)
      -> Int
      -> [(Int, a)]
      -> IO (MV.IOVector (Chunk get_handle a), [(Int, a)])

    read_mins _ v i acc
      | i == MV.length v
      = return (v, acc)

    read_mins chunk_size v i acc = do
      (mb_a, l) <-
        MV.read v i >>= peekML chunk_size

      MV.write v i l

      case mb_a of
        Nothing -> do
          -- Close the handle, move it to the end of the vector, shrink the
          -- vector
          close_in (_chunkHandle l)
          if (i == MV.length v - 1) then do
            -- Last element
            return (MV.slice 0 (MV.length v - 1) v, acc)
          else do
            MV.swap v i (MV.length v - 1)
            read_mins chunk_size (MV.slice 0 (MV.length v - 1) v) i acc
        Just a ->
          read_mins chunk_size v (i + 1) ((i, a) : acc)

--------------------------------------------------------------------------------


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
