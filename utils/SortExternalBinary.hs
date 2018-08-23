-- | Sort a binary file of Int list.

{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Main where

import qualified Data.Binary as B
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO
import Text.Read (readMaybe)

import ExternalSort

main :: IO ()
main = do
    args <- getArgs
    case args of
      [input, tmp_dir, chunk_size_str] ->
        maybe showUsage (run input tmp_dir) (readMaybe chunk_size_str)
      _ ->
        showUsage

showUsage :: IO ()
showUsage = do
    putStrLn "USAGE: sort-external <input> <tmp dir> <chunk size>"
    exitFailure

getInteger :: Get BinaryHandle Integer
getInteger = binaryGet B.get

putInteger :: Put Handle Integer
putInteger = binaryPut B.put

run :: FilePath -> FilePath -> Int -> IO ()
run input tmp_dir chunk_size =
    sortExternal chunk_size tmp_dir input
      (binaryGet B.get :: Get BinaryHandle Int)
      (binaryPut B.put :: Put Handle Int)
      (\f -> do
          h <- openFile f ReadMode
          return (BinaryHandle (Just h) [] 1000))
      (\f -> openFile f WriteMode)
      (\bh -> mapM_ hClose (_bhHandle bh))
      hClose
      "final"
