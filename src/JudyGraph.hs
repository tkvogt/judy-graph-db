{-# LANGUAGE DeriveGeneric, OverloadedStrings #-}
{-|
Module      :  JudyGraph
Description :  A fast and memory efficient graph library for dense graphs
Copyright   :  (C) 2017 Tillmann Vogt

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
then the functions in this module generate fast access node-edges with the "Graph.FastAccess" module.


-}
module JudyGraph (JGraph(..), Judy(..), Node(..), Edge(..),
                  -- * Construction
                  fromList, insertNode, insertNodes, insertEdge, insertEdges, union,
                  -- * Deletion
                  deleteNode, deleteNodes, deleteEdge,
                  -- * Query
                  isNull, lookupNode, lookupEdge,
                  -- * Changing node labels
                  mapNode, mapNodeWithKey,
                  -- * Cypher
                  (--|), (|--), (<--|), (|-->), (-~-), (-->), (<--), anyNode, executeOn
                 ) where

import           Control.Monad(foldM)
import qualified Data.Judy as J
import           Data.List.NonEmpty(NonEmpty(..))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(isJust, maybe, fromMaybe)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import JudyGraph.FastAccess(JGraph(..), Judy(..), NodeAttribute, EdgeAttribute, Node, Edge, RangeStart,
                  empty, fromListJudy, nullJ, buildWord64,
                  nodeWithLabel, nodeWithMaybeLabel, insertNodeEdges, updateNodeEdges,
                  deleteNodeJ, deleteEdgeJ,
                  unionJ, mapNodeJ, mapNodeWithKeyJ,
                  allChildEdges, allChildNodesFromEdges)
import JudyGraph.Cypher
import Debug.Trace

------------------------------------------------------------------------------------------------
-- Generation / Insertion

-- | If you don't need complex node/edge labels use 'fromListJudy'
fromList :: (NodeAttribute nl, EdgeAttribute el) =>
            [(Node, nl)] -> [(Edge, [el])] -> NonEmpty (RangeStart, nl) -> IO (JGraph nl el)
fromList nodes edges ranges = do
    jgraph <- empty ranges
    ngraph <- insertNodes jgraph nodes
    egraph <- insertEdges ngraph edges
    return egraph


-- | Inserting a new node means either
--
--  * if its new then only add an entry to the secondary data.map
--
--  * if it already exists then the label of the existing node is changed.
--    This can be slow because all node-edges have to be updated (O(#adjacentEdges))
insertNode :: NodeAttribute nl => JGraph nl el -> (Node, nl) -> IO (JGraph nl el)
insertNode jgraph (node, nl) = do
    let newNodeAttr = maybe Map.empty (Map.insert node nl) (complexNodeLabelMap jgraph)

    let enumNodeEdge = buildWord64 (nodeWithLabel node nl) 0 --the first index lookup is the count
    numEdges <- J.lookup enumNodeEdge (enumGraph jgraph)
    if isJust numEdges then do es <- allChildEdges jgraph node
                               ns <- allChildNodesFromEdges jgraph es node
                               mapM (updateNodeEdges jgraph node nl) (zip es ns)
                               return ()
                       else return ()
    return (jgraph { complexNodeLabelMap = Just newNodeAttr })

-- | Insert several nodes using 'insertNode'
insertNodes :: NodeAttribute nl => JGraph nl el -> [(Node, nl)] -> IO (JGraph nl el)
insertNodes jgraph nodes = do
    newGraph <- foldM insertNode jgraph nodes
    return newGraph    


insertEdge :: (NodeAttribute nl, EdgeAttribute el) =>
              JGraph nl el -> (Edge, [el]) -> IO (JGraph nl el)
insertEdge jgraph ((n0,n1), edgeLabels) = do
    insertNodeEdges jgraph ((n0, n1), nl0, nl1, edgeLabels)
    return (jgraph { complexEdgeLabelMap = Just newEdgeLabelMap })
  where
    nl0 = maybe Nothing (Map.lookup n0) (complexNodeLabelMap jgraph)
    nl1 = maybe Nothing (Map.lookup n1) (complexNodeLabelMap jgraph)

    -- Using the secondary map for more detailed data
    oldLabel = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap jgraph)
    newEdgeLabelMap = Map.insert (n0,n1)
                        ((fromMaybe [] oldLabel) ++ edgeLabels) -- multi edges between the same nodes
                        (fromMaybe Map.empty (complexEdgeLabelMap jgraph))


-- | Insert several edges using 'insertEdge'
insertEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                 JGraph nl el -> [(Edge, [el])] -> IO (JGraph nl el)
insertEdges jgraph edges = do
    newGraph <- foldM insertEdge jgraph edges
    return newGraph


--------------------------------------------------------------------------------------

-- | Make a union of two graphs by making a union of 'complexNodeLabelMap' and 'complexEdgeLabelMap'
-- but also calls 'unionJ' for a union of two judy arrays
union :: JGraph nl el -> JGraph nl el -> IO (JGraph nl el)
union (JGraph j0 enumJ0 complexNodeLabelMap0 complexEdgeLabelMap0 ranges0)
      (JGraph j1 enumJ1 complexNodeLabelMap1 complexEdgeLabelMap1 ranges1) = do

    (newJudyGraph, newJudyEnum) <- unionJ (j0, enumJ0) (j1, enumJ1)

    return (JGraph newJudyGraph newJudyEnum
            (mapUnion complexNodeLabelMap0 complexNodeLabelMap1)
            (mapUnion complexEdgeLabelMap0 complexEdgeLabelMap1)
            ranges0) -- assuming ranges are global
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
deleteNode :: (NodeAttribute nl, EdgeAttribute el) =>
              JGraph nl el -> Node -> IO (JGraph nl el)
deleteNode jgraph node = do
    let newNodeMap = maybe Nothing (Just . (Map.delete node)) (complexNodeLabelMap jgraph)
    deleteNodeJ jgraph node
    return (jgraph{ complexNodeLabelMap = newNodeMap })
  where
    nl = nodeWithMaybeLabel node (maybe Nothing (Map.lookup node) lmap)
    mj = graph jgraph
    lmap = complexNodeLabelMap jgraph


-- | This will currently produce holes in the continuously enumerated edge list of enumGraph.
--   But garbage collecting this is maybe worse.
deleteNodes :: (NodeAttribute nl, EdgeAttribute el) =>
               JGraph nl el -> [Node] -> IO (JGraph nl el)
deleteNodes jgraph nodes = do
    newNodeMap <- foldM deleteNode jgraph nodes
    return (jgraph{ complexNodeLabelMap = complexNodeLabelMap newNodeMap })


-- | This will currently produce holes in the continuously enumerated edge list of enumGraph
--   But garbage collecting this is maybe worse.
deleteEdge :: (NodeAttribute nl, EdgeAttribute el) =>
              JGraph nl el -> Edge -> IO (JGraph nl el)
deleteEdge jgraph (n0,n1) = do
    deleteEdgeJ jgraph n0 n1
    let newEdgeMap = maybe Nothing (Just . (Map.delete (n0,n1))) (complexEdgeLabelMap jgraph)
    return (jgraph { complexEdgeLabelMap = newEdgeMap })


----------------------------------------------------------------------------------------------------
-- Query

-- | Are the judy arrays and nodeLabelMap and edgeLabelMap empty
isNull :: JGraph nl el -> IO Bool
isNull (JGraph graph enumGraph nodeLabelMap edgeLabelMap rs) = do
  isNull <- nullJ (JGraph graph enumGraph nodeLabelMap edgeLabelMap rs)
  return (isNull && (maybe True Map.null nodeLabelMap)
                 && (maybe True Map.null edgeLabelMap))


-- | This function only works on 'complexNodeLabelMap'
lookupNode :: JGraph nl el -> Word32 -> Maybe nl
lookupNode graph n = maybe Nothing (Map.lookup n) (complexNodeLabelMap graph)


-- | This function only works on 'complexEdgeLabelMap'
lookupEdge :: JGraph nl el -> Edge -> Maybe [el]
lookupEdge graph (n0,n1) = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap graph)


---------------------------------------------------------------------------------------
-- Changing node labels

-- | This function only works on the secondary data.map structure
-- You have to figure out a function (Word32 -> Word32) that is equivalent to (nl -> nl)
-- and call mapNodeJ (so that everything stays consistent)
mapNode :: (nl -> nl) -> JGraph nl el -> IO (JGraph nl el)
mapNode f jgraph = do
  let newMap = maybe Nothing (Just . (Map.map f)) (complexNodeLabelMap jgraph)
  return (jgraph {complexNodeLabelMap = newMap})

-- | This function only works on the secondary data.map structure
-- You have to figure out a function (Node- > Word32 -> Word32) that is equivalent to (Node -> nl -> nl)
-- and call mapNodeWithKeyJ (so that everything stays consistent)
mapNodeWithKey :: (Node -> nl -> nl) -> JGraph nl el -> IO (JGraph nl el)
mapNodeWithKey f jgraph = do
  let newMap = maybe Nothing (Just . (Map.mapWithKey f)) (complexNodeLabelMap jgraph)
  return (jgraph {complexNodeLabelMap = newMap})

