-- | Generate a file with N random integers, one integer in a line.

{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Monad (replicateM_)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO
import System.Random (randomIO)
import Text.Read (readMaybe)

main :: IO ()
main = do
    args <- getArgs
    case args of
      [n, out] -> maybe showUsage (run out) (readMaybe n)
      _ -> showUsage

showUsage :: IO ()
showUsage = do
    putStrLn "USAGE: gen-test-file <size> <file>"
    exitFailure

run :: String -> Int -> IO ()
run file n =
    withFile file WriteMode $ \h ->
      replicateM_ n (randomIO >>= \(i :: Int) -> hPutStrLn h (show i))
