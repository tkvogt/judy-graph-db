module Main where

import           Data.Bits((.&.))
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Word(Word8, Word16, Word32)

import qualified JudyGraph as Graph
import JudyGraph(Judy) 
import JudyGraph.FastAccess


data EdgeLabel = W Word32  -- can be complex (like a record)
  deriving Show            -- figure out which attributes are important
                           -- for filtering edges

data NodeLabel = TN TypeNode -- can be complex (like several records)
               | FN FunctionNode
               | AN AppNode

type TypeNode = Word32
type FunctionNode = Word32
type AppNode = Word32


instance NodeAttribute NodeLabel where
    fastNodeAttr (TN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit


instance EdgeAttribute EdgeLabel where
    fastEdgeAttr el = (8,0) -- (Bits, Word32))
    fastEdgeAttrBase el = 0
    addCsvLine _ graph _ = return graph


main :: IO ()
main = do
  jgraph <- Graph.fromList nodes edges ranges
  putStrLn "done"

 where
  nodes :: [(Graph.Node, NodeLabel)]
  nodes = []

  edges :: [(Graph.Edge, [EdgeLabel])]
  edges = []

  ranges = NonEmpty.fromList [(0, TN 1), (10, FN 1), (20, AN 1)]
