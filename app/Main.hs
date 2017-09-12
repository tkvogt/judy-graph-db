module Main where

import qualified Data.Judy as J
import           Data.Word(Word8, Word16, Word32)

import qualified JudyGraph as Graph
import JudyGraph(Judy)


main :: IO ()
main = do
  j <- J.new :: IO Judy
  mj <- J.new :: IO Judy

  jgraph <- Graph.fromList j mj nodes edges
  putStrLn "done"

 where
  nodes :: [(Graph.Node, Graph.NodeLabel)]
  nodes = []

  edges :: [(Graph.Edge, [Graph.EdgeLabel])]
  edges = []
