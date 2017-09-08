module Main where

import qualified Data.Judy as J
import           Data.Word(Word8, Word16, Word32)

import qualified JudyGraph as Graph
import JudyGraph()


main :: IO ()
main = do
  j <- J.new :: IO (J.JudyL Word32)
  mj <- J.new :: IO (J.JudyL Word32)

  jgraph <- Graph.fromList j mj nodes edges
  putStrLn "done"

 where
  nodes :: [(Graph.Node, Graph.NodeLabel)]
  nodes = []

  edges :: [(Graph.Edge, [Graph.EdgeLabel])]
  edges = []
