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
               | AN AppNode deriving (Eq, Show)

type TypeNode = Word32
type FunctionNode = Word32
type AppNode = Word32

instance NodeAttribute NodeLabel where
    fastNodeAttr (TN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit
    fastNodeAttr (FN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit
    fastNodeAttr (AN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit

instance EdgeAttribute EdgeLabel where
    fastEdgeAttr (E n) = (8,n+1) -- (Bits, Word32))
    fastEdgeAttrBase el = 0

type CyN = CypherNode NodeLabel EdgeLabel
type CyE = CypherEdge NodeLabel EdgeLabel

instance Enum NodeLabel where
  toEnum i = TN (fromIntegral i)
  fromEnum (TN i) = fromIntegral i

main :: IO ()
main = do
  j <- J.new :: IO Judy
  mj <- J.new :: IO Judy


  -- graph with judy and data.map structures
  jgraph0 <- Graph.fromListE False nodes dirEdges [] ranges
  childEdgesN0 <- allChildEdges jgraph0 n0
  childNodesN0 <- allChildNodes jgraph0 n0
  childNodesFromEdgesN0 <- allChildNodesFromEdges jgraph0 n0 childEdgesN0
  adjacentCount <- JudyGraph.Enum.adjacentEdgeCount jgraph0 n0 e

  -- graph with only judy
  jgraph1 <- Graph.fromListE False nodes dirEdges [] ranges
  childEdgesJudyN0 <- allChildEdges jgraph1 n0
  childNodesJudyN0 <- allChildNodes jgraph1 n0
  childNodesFromEdgesJudyN0 <- allChildNodesFromEdges jgraph1 n0 childEdgesJudyN0
  adjacentCountJudy <- JudyGraph.Enum.adjacentEdgeCount jgraph1 n0 e

  -- insert
  jgraph2 <- empty ranges :: IO (EnumGraph NodeLabel EdgeLabel)
  jgraph3 <- insertNodeEdge False jgraph2 ((n1,n2), Nothing, Nothing, E 1, True)
  jgraph4 <- deleteEdge jgraph3 (n1,n2)
  jgraph5 <- insertNodeEdges False jgraph4 [] [ ((n1,n2), Nothing, Nothing, [E 1], True) ]
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

-- graph for cypher queries
--  cygraph  <- Graph.fromListE False nodes dirEdges  [] ranges
  cygraphR <- Graph.fromListE False nodes rightEdge [] ranges
  cygraphL <- Graph.fromListE False nodes leftEdge  [] ranges

  cyTable0 <- table cygraphR True (ns0 <--| next |--> ns1)
  cyTable1 <- table cygraphR True (ns0  --| next |--  ns1)
  cyTable2 <- table cygraphR True (ns0 <-- ns1)
  cyTable3 <- table cygraphR True (ns0 --> ns1)
  cyTable4 <- table cygraphR True (ns0 ~~  ns1)

  cyTable5 <- table cygraphL True (ns0 --> ns1)
  cyTable6 <- table cygraphL True (ns0 <-- ns1)
--  cyTable7 <- table cygraphL True (ns0 ~~  ns1)

  cyTable8  <- table cygraphR True (ns0 <--| ee |--  ns1)
  cyTable9  <- table cygraphR True (ns0  --| ee |--> ns1)

  cyTable10 <- table cygraphL True (ns0  --| ee |--> ns1)
  cyTable11 <- table cygraphL True (ns0 <--| ee |--  ns1)
{-
  cyTable12 <- table cygraphL True (ns0 <--| next |--  ns1)
  cyTable13 <- table cygraphL True (ns0 <--| next |--  ns1)
  cyTable14 <- table cygraphL True (ns0 <--| next |--  ns1)
-}
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
   describe "graph test\n" $ do
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
    it "Add two nodes, an edge that connects them. Is there one childnode of n0" $
      childNodesJudyN0 `shouldBe` [n1]
    it "Add two nodes, an edge that connects them. Is there one childedge of n0" $
      (length childEdgesJudyN0) `shouldBe` 1
    it "Add two nodes, an edge that connects them. Is there one childnode of n0 using the childedge" $
      (length childNodesFromEdgesJudyN0) `shouldBe` 1
    it "Add two nodes, an edge that connects them. Is there one childnode of n0 using the childedge" $
      adjacentCountJudy `shouldBe` 1
---------------
    it "Insert node. Then delete node. Result should be an empty graph" $
      (length childNodesFromEdgesN0) `shouldBe` 1
    it "Insert edge, but nodes haven't been inserted. Result should be an error." $
      (length childNodesFromEdgesN0) `shouldBe` 1
    it "Insert two nodes. Insert edge between them. " $
      (length childNodesFromEdgesN0) `shouldBe` 1
---------------

   describe "cypher queries with arrows\n" $ do

    it "0 An edge can only have one direction. Otherwise it is treated like an undirected edge" $
      cyTable0 `shouldBe` cyTable1

-- without edge specifier
    it "2 If the edge is to the right, but we query to the left, the query should be empty" $
      cyTable2 `shouldBe` []
    it "3 If the edge is to the right and we query to the right, there should be a result" $
      cyTable3 `shouldBe` [N (Ns [n0]),NE [], N (Ns [n1])]
    it "4 If the edge is to the right and we query without a dir, there should be a result" $
      cyTable4 `shouldBe` [N (Ns [n0]),NE [], N (Ns [n1])]
    it "5 If the edge is to the left, but we query to the right, the query should be empty" $
      cyTable5 `shouldBe` []
    it "6 If the edge is to the left and we query to the left, there should be a result" $
      cyTable6 `shouldBe` [N (Ns [n0]),NE [], N (Ns [n1])]

-- with edge specifier
    it "8 If the edge is to the right, but we query to the left, the query should be empty" $
      cyTable8 `shouldBe` []

    it "9 If the edge is to the right and we query to the right, there should be a result" $
      cyTable9 `shouldBe` [N (Ns [n0]),NE [], N (Ns [n1])]

    it "10 If the edge is to the left, but we query to the right, the query should be empty" $
      cyTable10 `shouldBe` []
    it "11 If the edge is to the left and we query to the left, there should be a result" $
      cyTable11 `shouldBe` [N (Ns [n0]),NE [], N (Ns [n1])]
{-
    it "An undirected query should get the same or more results than a directed query" $
      ((length cyTable2) <= (length cyTable4) &&
       (length cyTable3) <= (length cyTable4)) `shouldBe` True

    it "An undirected query should get the same or more results than a directed query" $
      ((length cyTable5) <= (length cyTable7) &&
       (length cyTable6) <= (length cyTable7)) `shouldBe` True
-}
{-
   describe "cypher queries and the behaviour of edge specifiers" $ do
    it "A table-query with edge specifiers" $
      (unsafePerformIO (table jgraph True (ns0 --| complexEdge1 |-- ns1))) `shouldBe`
      []

    it "A temp-query with edge specifiers" $
      (unsafePerformIO (temp jgraph True (ns0 --| complexEdge1 |-- ns1))) `shouldBe`
      []
-}
 where
  n0 = Node32 0
  n1 = Node32 1
  n2 = Node32 2
  n3 = Node32 2

  ns0 = node (nodes32 [0]) :: CyN
  ns1 = node (nodes32 [1]) :: CyN

  e = Edge32 0

  ranges = NonEmpty.fromList [(0, TN 1, [E 0]),
                              (10, FN 1, [E 0]),
                              (20, AN 1, [E 0])]

  nodes :: [(Graph.Node32, NodeLabel)]
  nodes = [(n0, FN 1), (n1, TN 2)]

  nodes1 :: [(Graph.Node32, NodeLabel)]
  nodes1 = [(n2, FN 3), (n3, AN 4)]

  nodesOverlap :: [(Graph.Node32, NodeLabel)]
  nodesOverlap = [(n1, FN 3), (n2, AN 4)]

  -- a right edge that is visible from n0
  dirEdges :: [(Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel], Bool)]
  dirEdges = [((n0,n1), Nothing, Nothing, [E 0], True)]

  -- a left edge that is visible from n1
  dirEdges1 :: [(Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel], Bool)]
  dirEdges1 = [((n1,n0), Nothing, Nothing, [E 0], True)]

  -- a right edge that is visible from both sides
  rightEdge :: [(Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel], Bool)]
  rightEdge = [((n0,n1), Nothing, Nothing, [E 0], True),
               ((n1,n0), Nothing, Nothing, [E 0], False)]

  -- a left edge that is visible from both sides
  leftEdge :: [(Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel], Bool)]
  leftEdge = [((n0,n1), Nothing, Nothing, [E 0], False),
              ((n1,n0), Nothing, Nothing, [E 0], True)]

  nodeEdges :: [(Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel], Bool)]
  nodeEdges = [((n0,n1), Just (TN 1), Just (FN 7), [E 0], True)]

  next = edge (***) :: CyE
  complexEdge1 = edge (***) :: CyE

  ee = edge (***) :: CyE

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
