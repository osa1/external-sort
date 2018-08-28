-- | Sort a binary file of Int list.

{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Main where

import qualified Data.Binary as B
import System.Environment (getArgs)
import System.Exit (exitFailure)
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

run :: FilePath -> FilePath -> Int -> IO ()
run input tmp_dir chunk_size =
    sortExternal compare chunk_size tmp_dir input
      (mkBinaryInHandle (B.get :: B.Get Int))
      (mkBinaryOutHandle B.put)
      "final"
