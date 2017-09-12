module Main where

import qualified Data.Judy as J
import qualified Data.Text as T
import Test.Hspec
import qualified JudyGraph as Graph
import JudyGraph(Judy)
import Debug.Trace

main :: IO ()
main = do
  j <- J.new :: IO Judy
  mj <- J.new :: IO Judy

  jgraph <- Graph.fromList j mj nodes edges

-- fromListJudy :: (NodeAttribute nl, EdgeAttribute el) =>
--                 Judy -> Judy -> [(Edge, Maybe nl, [el])] -> IO (JGraph nl el)

-- insertNode :: NodeAttribute nl => JGraph nl el -> (Node, nl) -> IO (JGraph nl el)

-- insertEdge :: (NodeAttribute nl, EdgeAttribute el) =>
--              JGraph nl el -> (Edge, [el]) -> IO (JGraph nl el)

-- insertEdgeSet :: (NodeAttribute nl, EdgeAttribute el) =>
--                  JGraph nl el -> Map Edge [el] -> IO (JGraph nl el)

-- insertNodeEdges :: (NodeAttribute nl, EdgeAttribute el) =>
--                    JGraph nl el -> (Edge, Maybe nl, [el]) -> IO ()

-- insertNodeEdge :: (NodeAttribute nl, EdgeAttribute el) =>
--                   JGraph nl el -> (Edge, Maybe nl, el) -> IO ()

-- union :: JGraph nl el -> JGraph nl el -> JGraph nl el

-- deleteNode :: NodeAttribute nl => JGraph nl el -> Node -> IO (JGraph nl el)

-- deleteNodeSet :: NodeAttribute nl => JGraph nl el -> Set Node -> IO (JGraph nl el)

-- deleteEdge :: JGraph nl el -> Edge -> IO (JGraph nl el)

-- deleteAllEdgesWithAttr :: (NodeAttribute nl, EdgeAttribute el) =>
--                            JGraph nl el -> el -> IO (JGraph nl el)

-- isEmpty :: JGraph nl el -> IO Bool

-- lookupNode :: JGraph nl el -> Word32 -> Maybe nl

-- lookupEdge :: JGraph nl el -> Edge -> Maybe [el]

-- adjacentNodesByAttr :: (NodeAttribute nl, EdgeAttribute el) =>
--                       JGraph nl el -> Node -> EdgeLabel -> IO (Set Word32)

-- lookupJudyNode :: Judy -> Node -> Word32 -> Word32 -> Word32 -> IO [Word32]

-- adjacentNodesByIndex :: JGraph nl el -> Word32 -> (Word32, Word32) -> IO (Set Word32)

-- adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Word32 -> IO Word32

-- allChildEdges :: JGraph nl el -> Word32 -> IO [Word32]

-- allChildNodes :: JGraph nl el -> Word32 -> IO [Word32]

-- allChildNodesFromEdges :: JGraph nl el -> [Word32] -> Word32 -> IO [Word32]

-- mapNode :: (nl0 -> nl1) -> JGraph nl el -> IO (JGraph nl el)

-- mapNodeWithKey :: (n -> nl0 -> nl1) -> JGraph nl el -> IO (JGraph nl el)

  hspec $ do
   describe "indexing" $ do
    it "Add two nodes(with labels), an edge that connects them(with label). Is there one childnode of n0" $
      1 `shouldBe` 1

    it "Add two nodes(with labels), an edge that connects them(with label). Is there one childedge of n0" $
      1 `shouldBe` 1

    it "Add two nodes(with labels), an edge that connects them(with label). Is there one childedge of n0 using the childedge" $
      1 `shouldBe` 1

    it "Add two nodes(with labels), an edge that connects them(with label). Is there one childedge of n0" $
      1 `shouldBe` 1

    it "inserts a single char twice in the trie and tries to find it" $
      2 `shouldBe` 2

 where
   nodes :: [(Graph.Node, Graph.NodeLabel)]
   nodes = []

   edges :: [(Graph.Edge, [Graph.EdgeLabel])]
   edges = []
