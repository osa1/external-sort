{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ExternalSort where

import Control.Monad
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Function (on)
import Data.Functor (($>))
import Data.List (minimumBy, sortBy)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import System.FilePath ((</>))
import System.IO

data InHandle a = forall state . InHandle
    { _ihState :: state
    , _ihGet   :: state -> IO (Maybe a, state)
    , _ihClose :: state -> IO ()
    }

data OutHandle a = forall state . OutHandle
    { _ohState :: state
    , _ohPut   :: state -> a -> IO state
    , _ohClose :: state -> IO ()
    }

ihGet :: InHandle a -> IO (Maybe a, InHandle a)
ihGet (InHandle state get close) = do
    (mb_a, state') <- get state
    return (mb_a, InHandle state' get close)

ihClose :: InHandle a -> IO ()
ihClose (InHandle state _ close) = close state

ohPut :: OutHandle a -> a -> IO (OutHandle a)
ohPut (OutHandle state put close) a = do
    state' <- put state a
    return (OutHandle state' put close)

ohClose :: OutHandle a -> IO ()
ohClose (OutHandle state _ close) = close state

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

mkBinaryInHandle :: B.Get a -> FilePath -> IO (InHandle a)
mkBinaryInHandle get f = do
    h <- openFile f ReadMode
    return InHandle
      { _ihState = BinaryHandle (Just h) [] 1000
      , _ihGet   = go (B.runGetIncremental get)
      , _ihClose = maybe (return ()) hClose . _bhHandle
      }
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

mkBinaryOutHandle :: (a -> B.Put) -> FilePath -> IO (OutHandle a)
mkBinaryOutHandle put f = do
    h0 <- openFile f WriteMode
    return OutHandle
      { _ohState = h0
      , _ohPut   = \h a -> LBS.hPut h (B.runPut (put a)) $> h
      , _ohClose = hClose
      }

sortExternal
    :: (a -> a -> Ordering)
    -> Int      -- ^ How many elements in a chunk?
    -> FilePath -- ^ Root directory for temporary files
    -> FilePath -- ^ File to sort
    -> (FilePath -> IO (InHandle a)) -- ^ How to open a file for getting
    -> (FilePath -> IO (OutHandle a)) -- ^ How to opena  file for putting
    -> FilePath -- ^ Where to put the sorted file
    -> IO ()

sortExternal cmp chunk_size tmp_dir file_in mk_get mk_put file_out = do
    -- Generate sorted chunk files
    n_chunks <- do
      h <- mk_get file_in
      ret <- generateChunks cmp h chunk_size tmp_dir mk_put
      ihClose h
      return ret

    -- Read chunk_size/(n_chunks + 1) sized chunks from each chunk, do
    -- n_chunks-way merge sort, write result to the output file.
    let sort_chunk_size = max (chunk_size `div` (n_chunks + 1)) 1

    -- TODO: These handles will leak on exception
    -- TODO: close handles
    hs <- forM [0..n_chunks-1] (\n_chunk -> mk_get (tmp_dir </> show n_chunk))
    h_out <- mk_put file_out

    h_out' <- merge' cmp hs sort_chunk_size h_out
    ohClose h_out'

--------------------------------------------------------------------------------
-- N-way merge. Load N chunks into memory, merge pair-wise. When a list is
-- completely merged, load more elements. If there are no more elements for that
-- list drop it.

data Chunk a = Chunk
    { _chunkElems  :: [a]
        -- ^ Elements of the list in memory.
    , _chunkHandle :: InHandle a
        -- ^ Handle to read more elements.
    , _chunkEOF    :: Bool
        -- ^ Reached eof?
    }

peekML :: Int -> Chunk a -> IO (Maybe a, Chunk a)
peekML chunk_size (Chunk lst in_h eof)
  | h : _ <- lst
  = return (Just h, Chunk lst in_h eof)

  | not eof
  = do (lst', in_h', eof') <- readChunk in_h chunk_size
       if null lst'
         then return (Nothing, Chunk [] in_h' eof')
         else peekML chunk_size (Chunk lst' in_h' eof')

  | otherwise
  = return (Nothing, Chunk [] in_h True)

unconsML :: Chunk a -> (a, Chunk a)
unconsML ml =
    case _chunkElems ml of
      []    -> error "unconsML: Empty list. Make sure you call peekML first."
      h : t -> (h, ml{ _chunkElems = t })

consML :: a -> Chunk a -> Chunk a
consML a l = l{ _chunkElems = a : _chunkElems l }

merge'
    :: forall a
     . (a -> a -> Ordering)
    -> [InHandle a]
    -> Int -- ^ Chunk size
    -> OutHandle a
    -> IO (OutHandle a)

merge' cmp ins chunk_size0 out_h0 = do
    -- A vector of lists to merge. Consumed lists are replaced with `Nothing`
    -- and moved to the end of the list.
    mls <- V.thaw (V.fromList (map mk_ml ins))

    go chunk_size0 mls out_h0
  where
    mk_ml in_h = Chunk [] in_h False

    go :: Int -- ^ Chunk size. Update as lists consumed.
       -> MV.IOVector (Chunk a)
              -- ^ Lists to merge
       -> OutHandle a
       -> IO (OutHandle a)

    go _ mls out_h
      | MV.null mls = return out_h

    go chunk_size mls out_h = do

      -- putStrLn ("Looping. Chunk size: " ++ show chunk_size)

      (mls', heads) <- read_mins chunk_size mls 0 []

      if null heads then
        return out_h
      else do
        let (min_idx, min_) = minimumBy (cmp `on` snd) heads

        min_l <- MV.read mls min_idx
        MV.write mls min_idx (snd (unconsML min_l))

        out_h' <- ohPut out_h min_

        go (max {- (chunk_size0 `div` MV.length mls') -} chunk_size0 1) mls' out_h'

    read_mins
      :: Int
      -> MV.IOVector (Chunk a)
      -> Int
      -> [(Int, a)]
      -> IO (MV.IOVector (Chunk a), [(Int, a)])

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
          ihClose (_chunkHandle l)
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
    :: forall a
     . (a -> a -> Ordering)
    -> InHandle a -- ^ File to split into chunks
    -> Int -- ^ Chunk size
    -> FilePath -- ^ Temp dir
    -> (FilePath -> IO (OutHandle a)) -- ^ How to open a file for put
    -> IO Int
generateChunks cmp h_in0 chunk_size tmp_dir mk_put =
    go 0 h_in0
  where
    go
      :: Int -- ^ Number of the current chunk file
      -> InHandle a
      -> IO Int
    go n_chunk h_in = do
      (chunk_unsorted, h_in', eof) <- readChunk h_in chunk_size
      h_out <- mk_put (tmp_dir </> show n_chunk)
      forM_ (sortBy cmp chunk_unsorted) (ohPut h_out)
      ohClose h_out
      if eof then return n_chunk else go (n_chunk + 1) h_in'

-- | Returns elements in the order they appear in the file and whether got
-- `Nothing` from `get`.
readChunk
    :: InHandle a
    -> Int -- ^ Size of the chunk
    -> IO ([a], InHandle a, Bool)
readChunk h0 chunk_size =
    go chunk_size h0
  where
    go 0 h =
      return ([], h, False)
    go n h =
      ihGet h >>= \case
        (Nothing, h') -> return ([], h', True)
        (Just i, h')  -> do
          (is, h'', eof) <- go (n - 1) h'
          return (i : is, h'', eof)
