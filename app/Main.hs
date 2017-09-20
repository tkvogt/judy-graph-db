module Main where

import           Data.Bits((.&.))
import qualified Data.Judy as J
import           Data.Word(Word8, Word16, Word32)

import qualified JudyGraph as Graph
import JudyGraph(Judy) 
import Graph.FastAccess


data EdgeLabel = Word32  -- can be complex (like a record)
                         -- figure out which attributes are important
                         -- for filtering edges

data NodeLabel = TN TypeNode -- can be complex (like several records)
               | FN FunctionNode
               | AN AppNode

type TypeNode = Word32
type FunctionNode = Word32
type AppNode = Word32


instance NodeAttribute NodeLabel where
    fastNodeAttr (TN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit

    fastNodeEdgeAttr (TN nl) el = (8,0) -- (Bits, Word32))
    fastNodeEdgeAttr (FN nl) el = (0,0)
    fastNodeEdgeAttr (AN nl) el = (0,0)


instance EdgeAttribute EdgeLabel where
    fastEdgeAttr jgraph node el = fastNodeEdgeAttr (nodeLabel jgraph node) el


main :: IO ()
main = do
  jgraph <- Graph.fromList nodes edges [(0, TN 1), (10, FN 1), (20, AN 1)]
  putStrLn "done"

 where
  nodes :: [(Graph.Node, NodeLabel)]
  nodes = []

  edges :: [(Graph.Edge, [EdgeLabel])]
  edges = []
