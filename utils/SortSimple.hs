-- | Sort a file with one integer per line (generate with gen-test-file) by
-- loading it into memory and `Data.List.sort`.

{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Exception (SomeException, catch)
import Control.Monad (forM_)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO
import Text.Read (readMaybe)

main :: IO ()
main = do
    args <- getArgs
    case args of
      [input] -> run input
      _       -> showUsage

showUsage :: IO ()
showUsage = do
    putStrLn "USAGE: sort-simple <input>"
    exitFailure

type Get a = Handle -> IO (Maybe a)
type Put a = Handle -> a -> IO ()

getInteger :: Get Integer
getInteger h =
    (readMaybe <$> hGetLine h)
      `catch` \(_ :: SomeException) -> return Nothing

putInteger :: Put Integer
putInteger h i = hPutStrLn h (show i)

run :: FilePath -> IO ()
run input = do
    lst <- withFile input ReadMode go
    withFile "final" WriteMode (\h -> forM_ lst (putInteger h))
  where
    go h = getInteger h >>= \case
             Nothing -> return []
             Just i  -> do
               is <- go h
               return (i : is)
