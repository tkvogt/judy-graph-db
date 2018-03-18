{-# LANGUAGE OverloadedStrings, FlexibleContexts, DeriveGeneric, DeriveAnyClass, Strict, 
    StrictData, MultiParamTypeClasses, FlexibleInstances, InstanceSigs #-}
{-|
Module      :  JudyGraph.Enum
Copyright   :  (C) 2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

-}
module JudyGraph.Enum (
    GraphClass(..), NodeAttribute(..), EdgeAttribute(..), JGraph(..), EnumGraph(..), Judy,
    Node, Edge, NodeEdge, EdgeAttr32, RangeStart, Index, Start, End, Bits(..),
    -- * Construction
    emptyE, emptyJ, fromListJ, fromListE,
    insertNodeEdge2, insertNodeEdgeAttr, insertNodeEdgeAttrE,
    insertCSVEdgeStream, insertNodeLines,
    updateNodeEdges, insertNE, mapNodeJ, mapNodeWithKeyJ,
    -- * Extraction
    getNodeEdges, nodeEdgesJ, nodesJ,
    -- * Deletion
    deleteNodeEdgeListJ, deleteNodeEdgeListE,
    -- * Query
    adjacentNodesByAttr, adjacentNodeByAttr, adjacentNodesByIndex,
    lookupJudyNodes, allChildEdges, allChildNodes, allChildNodesFromEdges, adjacentEdgeCount,
    -- * Handling Labels
    nodeWithLabel, nodeWithMaybeLabel, nodeLabel,
    hasNodeAttr, extrAttr, newNodeAttr, bitmask, invBitmask,
    buildWord64, extractFirstWord32, extractSecondWord32,
    -- * Displaying in hex for debugging
    showHex, showHex32
  ) where

import           Control.DeepSeq
import           Control.Monad.Error.Class
import           Control.Monad
import qualified Data.ByteString.Streaming as B
import qualified Data.ByteString.Char8 as C
import qualified Data.Char8 as C
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.List.NonEmpty(NonEmpty(..))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing, maybe, catMaybes, fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Lazy.IO as Text
import qualified Data.Text.Lazy as Text
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import           Streaming (Of, Stream)
import           Streaming.Cassava as S
import qualified Streaming.Prelude as S
import qualified Streaming.With as S
import           System.IO.Unsafe(unsafePerformIO)
import           JudyGraph.FastAccess
import Debug.Trace


-- | The edges are enumerated, because sometimes the edge attrs are not continuous
--   and it is impossible to try all possible 32 bit attrs
data (NodeAttribute nl, EdgeAttribute el) =>
     EnumGraph nl el = EnumGraph {
  judyGraphE :: Judy, -- ^ A Graph with 32 bit keys on the edge
  enumGraph :: Judy, -- ^ Enumerate the edges of the first graph, with counter at position 0.
                     --   Deletions in the first graph are not updated here (too costly)
  -- ^ An edge attr that doesn't fit into 64bit
  rangesE :: NonEmpty (RangeStart, nl), -- ^ a nonempty list with an attribute for every range
  nodeCountE :: Word32
}

------------------------------------------------------------------------------------------------

instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el) =>
         GraphClass EnumGraph nl el where
  -- | Generate two empty judy arrays and two empty data.maps for complex node and edge labels
  --
  -- The purpose of the range list is to give a special interpretation of edges
  -- depending on the node type.
  empty ranges = do
    j <- J.new :: IO Judy
    mj <- J.new :: IO Judy
    return (EnumGraph j mj ranges 0)

  -- | Are the judy arrays of the graph both empty?
  isNull (EnumGraph graph enumGraph _ _) = do
    g <- J.null graph
    e <- J.null enumGraph
    return (g && e)

  fromList overwrite nodes directedEdges nodeEdges ranges = do
    jgraph <- empty ranges
    insertNodeEdges overwrite jgraph (directedEdges ++ nodeEdges)

  -- | Build the graph without using the secondary Data.Map graph
  --   If edge already exists and (overwrite == True) overwrite it
  --   otherwise create a new edge and increase counter (that is at index 0)
  insertNodeEdge overwrite jgraph ((n0, n1), nl0, nl1, edgeLabel) =
    fmap fst $ insertNodeEdgeAttrE overwrite jgraph ((n0, n1), nl0, nl1, edgeAttr, edgeAttrBase)
   where
    edgeAttr = snd (fastEdgeAttr edgeLabel)
    edgeAttrBase = fastEdgeAttrBase edgeLabel


  union g0 g1 = do
    ((EnumGraph bg be br bn), (EnumGraph sg se sr sn)) <- biggerSmaller g0 g1
    nodeEs   <- getNodeEdges sg
    nodeEsMj <- getNodeEdges se
    insertNE nodeEs   bg
    insertNE nodeEsMj be
    return (EnumGraph bg be br bn)
   where
    biggerSmaller :: (NodeAttribute nl, EdgeAttribute el) =>
                     EnumGraph nl el -> EnumGraph nl el -> IO (EnumGraph nl el, EnumGraph nl el)
    biggerSmaller (EnumGraph g0 e0 r0 n0) (EnumGraph g1 e1 r1 n1) = do
       s0 <- J.size g0
       s1 <- J.size g1
       if s0 >= s1 then return ((EnumGraph g0 e0 r0 n0), (EnumGraph g1 e1 r1 n1))
                   else return ((EnumGraph g1 e1 r1 n1), (EnumGraph g0 e0 r0 n0))

  -- | Introduced for the cypher interface
  --   Makes a lookup to see how many edges there are
  -- TODO: Should they also lookup the target nodes?
  --       Currently Yes, just to make sure they exist
  adjacentEdgesByAttr jgraph node attr = do
    n <- J.lookup key j
    map fst <$> maybe (return []) (lookupJudyNodes j node attr 1) n --(Debug.Trace.trace ("eAdj "++ show n ++" "++ show node ++" "++ showHex32 attr ++" "++ showHex key) n)
   where
    key = buildWord64 node attr
    j = judyGraphE jgraph

  -- | deletes all node-edges that contain this node, because the judy array only stores node-edges
  deleteNode jgraph node = do
  --    es <- allChildEdges jgraph node
    let nodeEdges = map (buildWord64 node) [] -- es
    deleteNodeEdgeListE jgraph nodeEdges


  deleteNodes jgraph nodes = do
    newNodeMap <- foldM deleteNode jgraph nodes
    return jgraph

  -- | "deleteEdge jgraph (n0, n1)" deletes the edge that points from n0 to n1
  --
  --   It is slow because it uses 'filterEdgesTo'. If possible use 'deleteNodeEdgeList'.
  deleteEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                (EnumGraph nl el) -> Edge -> IO (EnumGraph nl el)
  deleteEdge jgraph (n0, n1) = do
    -- es <- allChildEdges jgraph n0
    let nodeEdges = map (buildWord64 n0) [] -- es
    edgesToN1 <- filterEdgesTo jgraph nodeEdges (== n1)
    deleteNodeEdgeListE jgraph edgesToN1


  -- | The Judy array maps a NodeEdge to a target node
  --
  --   Keep those NodeEdges where target node has a property (Word32 -> Bool)
  filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> J.lookup n j) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
   where j = judyGraphE jgraph
         filterNode (ne, Just v) | f v = True
                                 | otherwise = False
         filterNode _ = False

emptyE :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el) =>
          NonEmpty (RangeStart, nl) -> IO (EnumGraph nl el)
emptyE rs = empty rs

fromListE :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el) =>
             Bool -> [(Node, nl)] -> [(Edge, [el])]-> [(Edge, [el])] ->
             NonEmpty (RangeStart, nl) -> IO (EnumGraph nl el)
fromListE overwrite nodes dirEdges edges ranges =
  fromList overwrite nodes (addN dirEdges) (addN edges) ranges
    where addN = map (\(ns, es) -> (ns, Nothing, Nothing, es))

------------------------------------------------------------------------------------------------

-- | The input format consists of lines where each line consists of two unsigned Ints (nodes),
--   separated by whitespace
-- https://stackoverflow.com/questions/43570129/what-is-the-fastest-way-to-parse-line-with-lots-of-ints
insertNodeLines :: (NodeAttribute nl, EdgeAttribute el, Show el, Show nl) =>
                       EnumGraph nl el -> FilePath -> el -> IO (EnumGraph nl el)
insertNodeLines jgraph file edgeLabel =
 do a <- C.readFile file
    parse a
    return jgraph
  where
    parse s = do let ri = readInts s
                 if isJust ri then do
                   let ((n0,n1),rest) = fromJust ri
                   insertNodeEdge2 jgraph ((fromIntegral n0,
                                            fromIntegral n1), snd (fastEdgeAttr edgeLabel))
                   parse rest
                 else return ()

    readInts s = do (n0, s1) <- C.readInt s -- (Debug.Trace.trace ("0"++ [C.head s]) s)
                    let s2 =    C.dropWhile C.isSpace s1
                    (n1, s3) <- C.readInt s2
                    let s4 =    C.dropWhile C.isSpace s3
                    return ((n0, n1), s4)


insertNodeEdgeAttrE :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el) =>
                  Bool -> EnumGraph nl el -> (Edge, Maybe nl, Maybe nl, EdgeAttr32, EdgeAttr32)
                  -> IO (EnumGraph nl el, (Bool, Node))
insertNodeEdgeAttrE overwrite jgraph ((n0, n1), nl0, nl1, edgeAttr, edgeAttrBase) = do
    -- An edge consists of an attribute and a counter
    let edgeAttrCountKey = buildWord64 n0Key edgeAttrBase
    maybeEdgeAttrCount <- J.lookup edgeAttrCountKey j
    let edgeAttrCount = fromMaybe 0 maybeEdgeAttrCount

    insertEnumEdge jgraph n0Key (edgeAttr + if overwrite then 0 else edgeAttrCount)
--    debugToCSV (n0Key,n1Key) edgeLabel
    -------------------------------------
    let newValKey = buildWord64 n0Key (edgeAttr + if overwrite then 0 else edgeAttrCount)
    n2 <- J.lookup newValKey j
    let isEdgeNew = isNothing n2
    when (isEdgeNew || (not overwrite)) (J.insert edgeAttrCountKey (edgeAttrCount+1) j)
    J.insert newValKey n1Key j --(Debug.Trace.trace (show (n0, n1) ++" "++ show edgeAttrCount ++" "++ showHex newValKey ++"("++ showHex32 n0Key ++","++ showHex32 n1Key ++")"++ showHex32 edgeAttr ++ show nl1) j)
    if isEdgeNew || (not overwrite)
      then return (jgraph { nodeCountE = (nodeCountE jgraph) + 1}, (isEdgeNew, n1))
      else return (jgraph, (isEdgeNew, fromMaybe n1 n2))
  where
    j = judyGraphE jgraph
    n0Key = maybe n0 (nodeWithLabel n0) nl0
    n1Key = maybe n1 (nodeWithLabel n1) nl1


-- | Faster version of insertNodeEdge that has no counter for multiple edges 
--   from the same origin to the destination node. There are also no node labels.
insertNodeEdge2 :: (NodeAttribute nl, EdgeAttribute el) =>
                   EnumGraph nl el -> (Edge, EdgeAttr32) -> IO (EnumGraph nl el)
insertNodeEdge2 jgraph ((n0, n1), edgeAttr) = do
    let newValKey = buildWord64 n0 edgeAttr
    J.insert newValKey n1 (judyGraphE jgraph)
    return jgraph


insertEnumEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                  EnumGraph nl el -> Node -> Node -> IO ()
insertEnumEdge jgraph n0Key edgeAttr = do

    edgeCount <- J.lookup edgeCountKey mj
    if isNothing edgeCount
          then J.insert edgeCountKey 1 mj -- the first edge is added, set counter to 1
          else J.insert edgeCountKey ((fromJust edgeCount)+1) mj -- inc counter by 1
    let enumKey = buildWord64 n0Key ((fromMaybe 0 edgeCount)+1)
    J.insert enumKey edgeAttr mj
  where
    mj = enumGraph jgraph
    edgeCountKey = buildWord64 n0Key 0 -- the enumgraph has the number of adjacent edges at index 0


------------------------------------------------------------------------------------------
-- | A node-edge is deleted by deleting the key in the judy array.
--   Deleted edges that are pointed on by the enumgraph (the second judy array) are 
--   (planned!) lookup failures in the enumgraph. This is easier than some kind of
--   garbage collection.
--
--   If you delete node or edges a lot you should currently not use this library or rebuild
--   the graph regularly.

deleteNodeEdgeListE :: (NodeAttribute nl, EdgeAttribute el) =>
                      EnumGraph nl el -> [Word] -> IO (EnumGraph nl el)
deleteNodeEdgeListE jgraph ns = do
    mapM_ (\n -> J.delete n (judyGraphE jgraph)) ns
    return jgraph -- TODO counter?

----------------------------------------------------------------------------------
-- Query

-- | Useful if you want all adjacent edges, but you cannout check all 32 bit words with a lookup
-- and the distribution of the fast attributes follows no rule, there is no other choice but to 
-- enumerate all edges. Eg you reserve 24 bit for a unicode char, but only 50 chars are used.
-- But some algorithms need all adjacent edges, for example merging results of leaf nodes in a
-- tree recursively until the root is reached
adjacentNodesByIndex :: NodeAttribute nl => EnumGraph nl el -> Node -> (Start, End) -> IO [Node]
adjacentNodesByIndex jgraph node (start, end) = do
    val <- J.lookup key mj
    if isJust val then fmap (map snd) (lookupJudyNodes mj node 0 start end)
                  else return []
  where
    key = buildWord64 node 0
    mj = enumGraph jgraph


---------------------------------------------------------------------------
-- | The number of adjacent edges
adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) =>
                     EnumGraph nl el -> Node -> IO Word32
adjacentEdgeCount jgraph node = do
    -- the first index lookup is the count
    let edgeCountKey = buildWord64 node 0 -- (nodeWithMaybeLabel node nl) 0
    edgeCount <- J.lookup edgeCountKey mj
    return (fromMaybe 0 edgeCount)
  where
--    nl = maybe Nothing (Map.lookup node) (complexNodeLabelMap jgraph)
    mj = enumGraph jgraph


-- | The enumGraph enumerates all child edges
-- and maps to the second 32 bit of the key of all nodeEdges
allChildEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                 EnumGraph nl el -> Node -> IO [EdgeAttr32]
allChildEdges jgraph node = do
    edgeCount <- J.lookup edgeCountKey mj
    let ec = fromMaybe 0 edgeCount
    let enumKeys = map (buildWord64 node) [1..ec]
    edges <- mapM (\key -> J.lookup key mj) enumKeys
    return (-- Debug.Trace.trace ("ec " ++ show enumKeys) $
            catMaybes edges)
  where
    edgeCountKey = buildWord64 node 0
    mj = enumGraph jgraph


-- | all adjacent child nodes
allChildNodes :: (NodeAttribute nl, EdgeAttribute el) =>
                 EnumGraph nl el -> Node -> IO [Node]
allChildNodes jgraph node = do
    edges <- allChildEdges jgraph node
    allChildNodesFromEdges jgraph node edges


-- | To avoid the recalculation of edges
allChildNodesFromEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                          EnumGraph nl el -> Node -> [EdgeAttr32] -> IO [Node]
allChildNodesFromEdges jgraph node edges = do
    let keys = map (buildWord64 node) edges
    nodes <- mapM (\key -> J.lookup key j) keys
    return (-- Debug.Trace.trace ("ec " ++ show enumKeys) $
            catMaybes nodes)
  where
    j = judyGraphE jgraph

