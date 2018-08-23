{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ExternalSort
  ( sortExternal
  , Get
  , Put
  ) where

import Control.Monad (forM_, replicateM_, forM)
import Data.List (sort)
import System.FilePath ((</>))
import System.IO

type Get a = Handle -> IO (Maybe a)
type Put a = Handle -> a -> IO ()

sortExternal
    :: Ord a
    => Int      -- ^ How many elements in a chunk?
    -> FilePath -- ^ Root directory for temporary files
    -> FilePath -- ^ File to sort
    -> Get a    -- ^ How to read one element from a file
    -> Put a    -- ^ How to write one element to a file
    -> FilePath -- ^ Where to put the sorted file
    -> IO ()

sortExternal chunk_size tmp_dir file_in get put file_out = do
    -- Generate sorted chunk files
    n_chunks <- withFile file_in ReadMode (\h -> generateChunks h chunk_size tmp_dir get put)

    -- Read chunk_size/(n_chunks + 1) sized chunks from each chunk, do
    -- n_chunks-way merge sort, write result to the output file.
    let
      sort_chunk_size = max (chunk_size `div` (n_chunks + 1)) 1
      sort_steps = ceiling (fromIntegral chunk_size / fromIntegral (n_chunks + 1) :: Double) :: Int

    -- TODO: These handles will leak on exception
    hs <- forM [0..n_chunks-1] (\n_chunk -> openFile (tmp_dir </> show n_chunk) ReadMode)
    withFile file_out WriteMode $ \h_out -> do
      replicateM_ sort_steps $ do
        -- TODO: Drop files that return Nothing for the next iteration
        sorted_chunks <- forM hs (\h -> fst <$> readChunk h sort_chunk_size get)
        forM (merge sorted_chunks) (put h_out)
      forM_ hs hClose

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
    :: Ord a
    => Handle
    -> Int
    -> FilePath
    -> Get a
    -> Put a
    -> IO Int
generateChunks h_in chunk_size tmp_dir get put =
    go 0
  where
    go
      :: Int -- ^ Number of the current chunk file
      -> IO Int
    go n_chunk = do
      (chunk_unsorted, eof) <- readChunk h_in chunk_size get
      withFile (tmp_dir </> show n_chunk) WriteMode $ \h_out ->
        -- NOTE chunk files are sorted in reverse order
        forM_ (sort chunk_unsorted) (put h_out)
      if eof then return n_chunk else go (n_chunk + 1)

-- | Returns elements in the order they appear in the file and whether got
-- `Nothing` from `get`.
readChunk
    :: Handle -- ^ File to read the chunk from
    -> Int    -- ^ Size of the chunk
    -> Get a
    -> IO ([a], Bool)
readChunk h chunk_size get =
    go chunk_size
  where
    go 0 =
      return ([], False)
    go n =
      get h >>= \case
        Nothing -> return ([], True)
        Just i  -> do
          (is, eof) <- go (n - 1)
          return (i : is, eof)
