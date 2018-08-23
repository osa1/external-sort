-- | Sort a file with one integer per line (generate with gen-test-file) using
-- external-sorting.

{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Exception (SomeException, catch)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO
import Text.Read (readMaybe)

import ExternalSorting

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

getInteger :: Get Integer
getInteger h =
    (readMaybe <$> hGetLine h)
      `catch` \(_ :: SomeException) -> return Nothing

putInteger :: Put Integer
putInteger h i = hPutStrLn h (show i)

run :: FilePath -> FilePath -> Int -> IO ()
run input tmp_dir chunk_size =
    sortExternal chunk_size tmp_dir input getInteger putInteger "final"
