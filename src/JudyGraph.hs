{-# LANGUAGE DeriveGeneric, OverloadedStrings, UnicodeSyntax, MultiParamTypeClasses,
    FlexibleInstances, InstanceSigs, ScopedTypeVariables #-}
{-|
Module      :  JudyGraph
Description :  A fast and memory efficient graph library for dense graphs
Copyright   :  (C) 2017-2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

JudyGraph uses Judy arrays as a fast key-value storage (key: 64 bit, value: 32 bit).

This module contains functions that use the secondary structures 'complexNodeLabelMap' and
'complexEdgeLabelMap' for more complex node/edge-labels that don't fit into 32 bit.
You only have to write class instances that specify how node and edge labels are
converted into 32 bit labels: 'NodeAttribute', 'EdgeAttribute'. This shrinking obviously has 
to keep the information that is needed to quickly query the graph.
E.g. if you have e.g. 100.000 edges adjacent to a node and you
don't want to test them all for a certain label.

If you want to save memory and insertion-time and
if you can put each node/edge-label in less than 32 bit
then use only functions in the "Graph.FastAccess" module

If on the other hand there is enough space and the nodes or edges have much more info 
then the functions in this module generate fast access node-edges with the "Graph.FastAccess"
module.

-}
module JudyGraph (JGraph(..), EnumGraph(..), Judy(..), Node(..), Edge(..),
      -- * Construction
      ComplexGraph(..), emptyJ, emptyE, fromList, fromListJ, fromListE, insertCSVEdgeStream,
      insertNode, insertNodes, insertNodeLines, insertNodeEdge, insertNodeEdges, union,
      -- * Deletion
      deleteNode, deleteNodes, deleteEdge,
      -- * Query
      isNull, lookupNode, lookupEdge,
      -- * Changing node labels
      mapNodeJ, mapNodeWithKeyJ,
      -- * Cypher Query
      QueryN(..), QueryNE(..),
      -- * Cypher Query with Unicode
      (─┤),  (├─),  (<─┤),  (├─>),  (⟞⟝), (⟼),  (⟻),
      -- * Query Components
      CypherComp(..), CypherNode(..), CypherEdge(..),
      -- * Query Evaluation
      Table(..), GraphCreateReadUpdate(..),
      -- * Setting of Attributes, Labels,...
      Attr(..), LabelNodes(..), 
      -- * type classes for translation into bits
      NodeAttribute(..), EdgeAttribute(..),
      -- * Unevaluated node/edge markers
      anyNode, nodes32, edge, node, attr, labels, where_, several, (…),
      -- * evaluating, changing markers
      evalNode, appl
     ) where

import           Control.Monad(foldM, when)
import qualified Data.Judy as J
import           Data.List.NonEmpty(NonEmpty(..))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(isJust, maybe, fromMaybe)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import JudyGraph.Enum(GraphClass(..), JGraph(..), EnumGraph(..), Judy(..),
                  NodeAttribute(..), EdgeAttribute(..), EdgeAttr32, Node, Edge,
                  RangeStart, emptyJ, emptyE, fromList, fromListJ, fromListE, isNull,
                  insertCSVEdgeStream,
                  buildWord64, nodeWithLabel, nodeWithMaybeLabel, updateNodeEdges,
                  insertNodeEdges, insertNodeLines,
                  deleteNode, deleteEdge,
                  union, mapNodeJ, mapNodeWithKeyJ,
                  allChildEdges, allChildNodesFromEdges, lookupJudyNodes)
import JudyGraph.Cypher
import Debug.Trace


-- | If the graph contains data that doesn't fit into 32 bit node/edges and there is enough memory
data (NodeAttribute nl, EdgeAttribute el) =>
     ComplexGraph nl el = ComplexGraph {
  judyGraphC :: Judy, -- ^ A Graph with 32 bit keys on the edge
  enumGraphC :: Judy, -- ^ Enumerate the edges of the first graph, with counter at position 0.
                     --   Deletions in the first graph are not updated here (too costly)
  complexNodeLabelMap :: Maybe (Map Word32 nl), -- ^ A node attr that doesn't fit into 64bit
  complexEdgeLabelMap :: Maybe (Map (Node,Node) [el]),
  -- ^ An edge attr that doesn't fit into 64bit
  rangesC :: NonEmpty (RangeStart, nl), -- ^ a nonempty list with an attribute for every range
  nodeCountC :: Word32
}


-- | Inserting a new node means either
--
--  * if its new then only add an entry to the secondary data.map
--
--  * if it already exists then the label of the existing node is changed.
--    This can be slow because all node-edges have to be updated (O(#adjacentEdges))
insertNode :: (NodeAttribute nl, EdgeAttribute el) =>
              ComplexGraph nl el -> (Node, nl) -> IO (ComplexGraph nl el)
insertNode (ComplexGraph j eg nm em r n) (node, nl) = do
  let newNodeAttr = maybe Map.empty (Map.insert node nl) nm

  -- the first index lookup is the count
  let enumNodeEdge = buildWord64 (nodeWithLabel node nl) 0
  numEdges <- J.lookup enumNodeEdge eg
--  when (isJust numEdges) $ do
--       es <- allChildEdges (EnumGraph j eg r n) node
            -- :: (NodeAttribute nl, EdgeAttribute el) => IO [EdgeAttr32]
--       ns <- allChildNodesFromEdges (EnumGraph j eg r n :: (NodeAttribute nl, EdgeAttribute el) => EnumGraph nl el) node es
--       mapM_ (updateNodeEdges (JGraph j r n) node nl) [] -- (zip es ns)
  return (ComplexGraph j eg (Just newNodeAttr) em r n)
-- where
--  ace = allChildEdges (EnumGraph j eg r n) node

-- | Insert several nodes using 'insertNode'
insertNodes :: (NodeAttribute nl, EdgeAttribute el) =>
               ComplexGraph nl el -> [(Node, nl)] -> IO (ComplexGraph nl el)
insertNodes jgraph nodes = foldM insertNode jgraph nodes


instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
         GraphClass ComplexGraph nl el where

  empty ranges = do
    j <- J.new :: IO Judy
    e <- J.new :: IO Judy
    let nl = Map.empty :: Map Word32 nl
    let el = Map.empty :: Map (Node,Node) [el]
    return (ComplexGraph j e (Just nl) (Just el) ranges 0)


  -- | Are the judy arrays and nodeLabelMap and edgeLabelMap empty
  isNull :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
            ComplexGraph nl el -> IO Bool
  isNull (ComplexGraph graph enumGraph nodeLabelMap edgeLabelMap rs n) = do
    isN <- isNull (EnumGraph graph enumGraph rs n ::
                       (NodeAttribute nl, EdgeAttribute el) => EnumGraph nl el)
    return (isN && (maybe True Map.null nodeLabelMap)
                && (maybe True Map.null edgeLabelMap))


  -- | If you don't need complex node/edge labels use 'fromListJudy'
  fromList overwrite nodes directedEdges edges ranges = do
      jgraph <- empty ranges
      ngraph <- insertNodes jgraph nodes
      insertNodeEdges overwrite ngraph (directedEdges ++ edges ++ (map rev edges))
    where rev ((from,to), nl0, nl1, labels) = ((to,from), nl1, nl0, labels)


  insertNodeEdge overwrite jgraph ((n0,n1), _, _, edgeLabels) = do
    insertNodeEdge overwrite jgraph ((n0, n1), nl0, nl1, edgeLabels)
    return (jgraph { complexEdgeLabelMap = Just newEdgeLabelMap })
   where
    nl0 = maybe Nothing (Map.lookup n0) (complexNodeLabelMap jgraph)
    nl1 = maybe Nothing (Map.lookup n1) (complexNodeLabelMap jgraph)

    -- Using the secondary map for more detailed data
    oldLabel = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap jgraph)
    newEdgeLabelMap = Map.insert (n0,n1)
             ((fromMaybe [] oldLabel) ++ [edgeLabels]) -- multi edges between the same nodes
             (fromMaybe Map.empty (complexEdgeLabelMap jgraph))

  --------------------------------------------------------------------------------------

  -- | Make a union of two graphs by making a union of 'complexNodeLabelMap' and 
  --   'complexEdgeLabelMap' but also calls 'unionJ' for a union of two judy arrays
  union :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
           ComplexGraph nl el -> ComplexGraph nl el -> IO (ComplexGraph nl el)
  union (ComplexGraph j0 enumJ0 complexNodeLabelMap0 complexEdgeLabelMap0 ranges0 n0)
        (ComplexGraph j1 enumJ1 complexNodeLabelMap1 complexEdgeLabelMap1 ranges1 n1) = do

    (EnumGraph newJGraph newJEnum nm em :: EnumGraph nl el)
        <- union (EnumGraph j0 enumJ0 ranges0 n0) (EnumGraph j1 enumJ1 ranges1 n1)

    return (ComplexGraph newJGraph newJEnum
            (mapUnion complexNodeLabelMap0 complexNodeLabelMap1)
            (mapUnion complexEdgeLabelMap0 complexEdgeLabelMap1)
            ranges0 -- assuming ranges are global
            (n0+n1))
   where
    mapUnion (Just complexLabelMap0)
             (Just complexLabelMap1) = Just (Map.union complexLabelMap0 complexLabelMap1)
    mapUnion Nothing (Just complexLabelMap1) = Just complexLabelMap1
    mapUnion (Just complexLabelMap0) Nothing = Just complexLabelMap0
    mapUnion Nothing Nothing = Nothing


  ---------------------------------------------------------------------------------------
  -- Deletion
  -- Caution: Should currently be used much less than insert.
  --          It can make the lookup slower because of lookup failures

  -- | This will currently produce holes in the continuously enumerated edge list of enumGraph.
  --   But garbage collecting this is maybe worse.
  deleteNode jgraph node = do
    let newNodeMap = fmap (Map.delete node) (complexNodeLabelMap jgraph)
    deleteNode jgraph node
    return (jgraph{ complexNodeLabelMap = newNodeMap })
   where
    nl = nodeWithMaybeLabel node (maybe Nothing (Map.lookup node) lmap)
    mj = judyGraphC jgraph
    lmap = complexNodeLabelMap jgraph


  -- | This will currently produce holes in the continuously enumerated edge list of enumGraph.
  --   But garbage collecting this is maybe worse.
  deleteNodes jgraph nodes = do
    newNodeMap <- foldM deleteNode jgraph nodes
    return (jgraph{ complexNodeLabelMap = complexNodeLabelMap newNodeMap })


  -- | This will currently produce holes in the continuously enumerated edge list of enumGraph
  --   But garbage collecting this is maybe worse.
  deleteEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                (ComplexGraph nl el) -> Edge -> IO (ComplexGraph nl el)
  deleteEdge (ComplexGraph j e nm em r n) (n0,n1) = do
--    deleteEdge (JGraph j r n) (n0, n1)
    let newEdgeMap = fmap (Map.delete (n0,n1)) (complexEdgeLabelMap (ComplexGraph j e nm em r n))
    return (ComplexGraph j e nm newEdgeMap r n)


  adjacentEdgesByAttr jgraph node attr = do
    n <- J.lookup key j
    map fst <$> maybe (return []) (lookupJudyNodes j node attr 1) n
   where
    key = buildWord64 node attr
    j = judyGraphC jgraph


  allChildEdges jgraph node = do
    return []


  filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> J.lookup n j) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
   where j = judyGraphC jgraph
         filterNode (ne, Just v) | f v = True
                                 | otherwise = False
         filterNode _ = False


  nodeCount graph = nodeCountC graph
  ranges :: Enum nl => ComplexGraph nl el -> NonEmpty (RangeStart, nl)
  ranges graph = rangesC graph
  judyGraph graph = judyGraphC graph

---------------------------------------------------------------------------------------
-- Changing node labels

-- | This function only works on the secondary data.map structure
-- You have to figure out a function (Word32 -> Word32) that is equivalent to (nl -> nl)
-- and call mapNodeJ (so that everything stays consistent)
mapNode :: (NodeAttribute nl, EdgeAttribute el) =>
           (nl -> nl) -> ComplexGraph nl el -> IO (ComplexGraph nl el)
mapNode f jgraph = do
  let newMap = fmap (Map.map f) (complexNodeLabelMap jgraph)
  return (jgraph {complexNodeLabelMap = newMap})

-- | This function only works on the secondary data.map structure
-- You have to figure out a function (Node- > Word32 -> Word32) that is equivalent 
-- to (Node -> nl -> nl) and call mapNodeWithKeyJ (so that everything stays consistent)
mapNodeWithKey :: (NodeAttribute nl, EdgeAttribute el) =>
                  (Node -> nl -> nl) -> (ComplexGraph nl el) -> IO (ComplexGraph nl el)
mapNodeWithKey f jgraph = do
  let newMap = fmap (Map.mapWithKey f) (complexNodeLabelMap jgraph)
  return (jgraph {complexNodeLabelMap = newMap})

--------------------------------------------------------------------------------------------
-- Query

-- | This function only works on 'complexNodeLabelMap'
lookupNode :: (NodeAttribute nl, EdgeAttribute el) =>
              ComplexGraph nl el -> Word32 -> Maybe nl
lookupNode graph n = maybe Nothing (Map.lookup n) (complexNodeLabelMap graph)


-- | This function only works on 'complexEdgeLabelMap'
lookupEdge :: (NodeAttribute nl, EdgeAttribute el) =>
              ComplexGraph nl el -> Edge -> Maybe [el]
lookupEdge graph (n0,n1) = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap graph)

--------------------------------------------------------------------------------------------

instance (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
         Table ComplexGraph nl el (CypherNode nl el) where
  table graph cypherNode
      | null (cols0 cypherNode) =
          do (CypherNode a n evalN) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             evalToTable graph [CN (CypherNode a n True)]
      | otherwise = evalToTable graph (reverse (cols0 cypherNode))

  temp graph cypherNode
      | null (cols0 cypherNode) =
          do (CypherNode a n evalN) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             return [CN (CypherNode a n False)]
      | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                         (runOn graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  createMem graph cypherNode
      | null (cols0 cypherNode) = return (GraphDiff [] [] [] []) -- TODO
      | otherwise = fmap snd (runOn graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

instance (Eq nl, Show nl, Show el, NodeAttribute nl, Enum nl, EdgeAttribute el) =>
         Table ComplexGraph nl el (CypherEdge nl el) where
  table graph cypherEdge | null (cols1 cypherEdge) = return []
                         | otherwise = evalToTable graph (reverse (cols1 cypherEdge))

  temp graph cypherEdge
      | null (cols1 cypherEdge) = return []
      | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                         (runOn graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  createMem graph cypherEdge
      | null (cols1 cypherEdge) = return (GraphDiff [] [] [] [])
      | otherwise = fmap snd (runOn graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

