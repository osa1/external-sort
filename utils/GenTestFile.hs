-- | Generate a file with N random integers, one integer in a line.

{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Monad (replicateM_)
import qualified Data.Binary as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString.Lazy as LBS
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.FilePath
import System.IO
import System.Random (randomIO)
import Text.Read (readMaybe)

main :: IO ()
main = do
    args <- getArgs
    case args of
      [n, out] -> maybe showUsage (run out) (readMaybe n)
      _        -> showUsage

showUsage :: IO ()
showUsage = do
    putStrLn "USAGE: gen-test-file <size> <file>"
    exitFailure

run :: String -> Int -> IO ()
run file n =
    withFile file WriteMode $ \h ->
      withFile (file <.> "bin") WriteMode $ \h_bin -> do
        -- LBS.hPut h_bin (B.runPut (B.put n))
          -- ^ this won't work as it makes get and put asymmetric
        replicateM_ n $ do
          i :: Int <- randomIO
          hPutStrLn h (show i)
          LBS.hPut h_bin (B.runPut (B.put i))
