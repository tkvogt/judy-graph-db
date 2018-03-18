module Main where

import           Data.Bits((.&.))
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map as Map
import qualified Data.Text as T
import           Data.Word(Word32)
import           Test.Hspec

import qualified JudyGraph.Enum as Graph
import           JudyGraph.Enum
import qualified JudyGraph as Graph
import           JudyGraph
import Debug.Trace


data EdgeLabel = E Word32 deriving Show -- can be complex (like a record)
                                        -- figure out which attributes are important
                                        -- for filtering edges

data NodeLabel = TN TypeNode -- can be complex (like several records)
               | FN FunctionNode
               | AN AppNode deriving Show

type TypeNode = Word32
type FunctionNode = Word32
type AppNode = Word32

instance NodeAttribute NodeLabel where
    fastNodeAttr (TN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit
    fastNodeAttr (FN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit
    fastNodeAttr (AN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit


instance EdgeAttribute EdgeLabel where
    fastEdgeAttr el = (8,0) -- (Bits, Word32))
    fastEdgeAttrBase el = 0
    edgeForward = Nothing
    addCsvLine _ graph _ = return graph

main :: IO ()
main = do
  j <- J.new :: IO Judy
  mj <- J.new :: IO Judy

  -- graph with judy and data.map structures
  jgraph0 <- Graph.fromListE False nodes dirEdges [] ranges
  childEdgesN0 <- allChildEdges jgraph0 n0
  childNodesN0 <- allChildNodes jgraph0 n0
  childNodesFromEdgesN0 <- allChildNodesFromEdges jgraph0 n0 childEdgesN0
  adjacentCount <- adjacentEdgeCount jgraph0 n0

  -- graph with only judy
  jgraph1 <- Graph.fromListE False nodes dirEdges [] ranges
  childEdgesJudyN0 <- allChildEdges jgraph1 n0
  childNodesJudyN0 <- allChildNodes jgraph1 n0
  childNodesFromEdgesJudyN0 <- allChildNodesFromEdges jgraph1 n0 childEdgesJudyN0
  adjacentCountJudy <- adjacentEdgeCount jgraph1 n0

  -- insert
  jgraph2 <- empty ranges :: IO (EnumGraph NodeLabel EdgeLabel)
  jgraph3 <- insertNodeEdge False jgraph2 ((n1,n2), Nothing, Nothing, E 1)
  jgraph4 <- deleteEdge jgraph3 (n1,n2)
  jgraph5 <- insertNodeEdges False jgraph4 [ ((n1,n2), Nothing, Nothing, [E 1]) ]
  jgraph6 <- deleteNode jgraph5 n1

-- union of empty graphs
  ugraph0 <- empty ranges :: IO (EnumGraph NodeLabel EdgeLabel)
  ugraph1 <- empty ranges :: IO (EnumGraph NodeLabel EdgeLabel)
  ugraph2 <- ugraph0 `union` ugraph1
  emptyUnion <- isNull ugraph2

-- union of non-empty graphs
  ograph0 <- Graph.fromListE False nodes dirEdges [] ranges
  ograph1 <- Graph.fromListE False nodesOverlap dirEdges [] ranges
  ograph2 <- ugraph0 `union` ugraph1
  overlapUnion <- isNull ograph2

  

-- union :: JGraph nl el -> JGraph nl el -> IO (JGraph nl el)
-- isEmpty :: JGraph nl el -> IO Bool
-- lookupNode :: JGraph nl el -> Word32 -> Maybe nl
-- lookupEdge :: JGraph nl el -> Edge -> Maybe [el]
-- adjacentNodesByAttr :: (NodeAttribute nl, EdgeAttribute el) =>
--                       JGraph nl el -> Node -> EdgeLabel -> IO (Set Word32)
-- lookupJudyNode :: Judy -> Node -> Word32 -> Word32 -> Word32 -> IO [Word32]
-- adjacentNodesByIndex :: JGraph nl el -> Word32 -> (Word32, Word32) -> IO (Set Word32)
-- mapNode :: (nl0 -> nl1) -> JGraph nl el -> IO (JGraph nl el)
-- mapNodeWithKey :: (n -> nl0 -> nl1) -> JGraph nl el -> IO (JGraph nl el)

  hspec $ do
   describe "graph test" $ do

-- nodes and edges with fromList

    it "Add two nodes, an edge that connects them. Is there one childnode of n0" $
      childNodesN0 `shouldBe` [n1]

    it "Add two nodes, an edge that connects them. Is there one childedge of n0" $
      (length childEdgesN0) `shouldBe` 1

    it "Add two nodes, an edge that connects them. Is there one childnode of n0 using the childedge" $
      (length childNodesFromEdgesN0) `shouldBe` 1

    it "Add two nodes, an edge that connects them. Is there one childnode of n0 using the childedge" $
      adjacentCount `shouldBe` 1

-- nodes and edges with fromListJudy
   describe "graph test" $ do
    it "Add two nodes, an edge that connects them. Is there one childnode of n0" $
      childNodesJudyN0 `shouldBe` [n1]

    it "Add two nodes, an edge that connects them. Is there one childedge of n0" $
      (length childEdgesJudyN0) `shouldBe` 1

    it "Add two nodes, an edge that connects them. Is there one childnode of n0 using the childedge" $
      (length childNodesFromEdgesJudyN0) `shouldBe` 1

    it "Add two nodes, an edge that connects them. Is there one childnode of n0 using the childedge" $
      adjacentCountJudy `shouldBe` 1

---------

    it "Insert node. Then delete node. Result should be an empty graph" $
      (length childNodesFromEdgesN0) `shouldBe` 1

    it "Insert edge, but nodes haven't been inserted. Result should be an error." $
      (length childNodesFromEdgesN0) `shouldBe` 1

    it "Insert two nodes. Insert edge between them. " $
      (length childNodesFromEdgesN0) `shouldBe` 1

 where
  n0 = 0
  n1 = 1
  n2 = 2
  n3 = 2

  ranges = NonEmpty.fromList [(0, TN 1), (10, FN 1), (20, AN 1)]

  nodes :: [(Graph.Node, NodeLabel)]
  nodes = [(n0, FN 1), (n1, TN 2)]

  nodes1 :: [(Graph.Node, NodeLabel)]
  nodes1 = [(n2, FN 3), (n3, AN 4)]

  nodesOverlap :: [(Graph.Node, NodeLabel)]
  nodesOverlap = [(n1, FN 3), (n2, AN 4)]

  dirEdges :: [(Graph.Edge, [EdgeLabel])]
  dirEdges = [((n0,n1), [E 1])]

  nodeEdges :: [(Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel])]
  nodeEdges = [((n0,n1), Just (TN 1), Just (FN 7), [E 1])]

-- fromListJudy :: (NodeAttribute nl, EdgeAttribute el) =>
--                 Judy -> Judy -> [(Edge, Maybe nl, [el])] -> IO (JGraph nl el)
-- adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Word32 -> IO Word32
-- allChildEdges :: JGraph nl el -> Word32 -> IO [Word32]
-- allChildNodes :: JGraph nl el -> Word32 -> IO [Word32]
-- allChildNodesFromEdges :: JGraph nl el -> [Word32] -> Word32 -> IO [Word32]
-- insertNode :: NodeAttribute nl => JGraph nl el -> (Node, nl) -> IO (JGraph nl el)
-- insertEdge :: (NodeAttribute nl, EdgeAttribute el) =>
--              JGraph nl el -> (Edge, [el]) -> IO (JGraph nl el)
-- insertEdgeSet :: (NodeAttribute nl, EdgeAttribute el) =>
--                  JGraph nl el -> Map Edge [el] -> IO (JGraph nl el)
-- insertNodeEdges :: (NodeAttribute nl, EdgeAttribute el) =>
--                    JGraph nl el -> (Edge, Maybe nl, [el]) -> IO ()
-- insertNodeEdge :: (NodeAttribute nl, EdgeAttribute el) =>
--                   JGraph nl el -> (Edge, Maybe nl, el) -> IO ()
-- deleteNode :: NodeAttribute nl => JGraph nl el -> Node -> IO (JGraph nl el)
