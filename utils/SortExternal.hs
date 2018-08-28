-- | Sort a file with one integer per line (generate with gen-test-file) using
-- external-sort.

{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Main where

import Control.Exception (SomeException, catch)
import Data.Functor (($>))
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

mkInHandle :: FilePath -> IO (InHandle Integer)
mkInHandle f = do
    h0 <- openFile f ReadMode
    return InHandle
      { _ihState = h0
      , _ihGet   = \h ->
          ((, h) . readMaybe <$> hGetLine h)
            `catch` \(_ :: SomeException) -> return (Nothing, h)
      , _ihClose = hClose
      }

mkOutHandle :: FilePath -> IO (OutHandle Integer)
mkOutHandle f = do
    h0 <- openFile f WriteMode
    return OutHandle
      { _ohState = h0
      , _ohPut   = \h a -> hPutStrLn h (show a) $> h
      , _ohClose = hClose
      }

run :: FilePath -> FilePath -> Int -> IO ()
run input tmp_dir chunk_size =
    sortExternal compare chunk_size tmp_dir input mkInHandle mkOutHandle "final"
