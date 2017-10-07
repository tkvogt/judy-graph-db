{-# LANGUAGE DeriveGeneric, OverloadedStrings #-}
{-|
Module      :  Graph.FastAccess
Copyright   :  (C) 2017 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

This module only contains functions that don't use the secondary data.map structures.
It should be memory efficient and fast.

A graph consists of nodes and edges, that point from one node to another.
To encode this with a key-value storage (Judy Arrays), we encode the key as a (node,edge)-pair
and the value as a node:

@
       key           -->   value

    node | edge            node
  ---------------    -->   ------
  32 bit | 32 bit          32 bit
@

To check attributes of edges and nodes fast, we allow to use some of the node/edge bits for these
attributes. This is called fastAttr and can be up to 32 bits. The number of fastAttr bits in the
node has to be constant, because you are either wasting space or overflowing.
It depends on your usage scenario if you plan to use 4 bilion nodes (2^32) or if you need to check 
attributes of a lot of nodes/edges quickly and for example 2^24 nodes are enough, having 8 bits
to encode an attribute:

@
    node | node attr |  edge  | edge attr           node
  ----------------------------------------   -->   ------
  24 bit |  8 bit    | 16 bit | 16 bit             32 bit
@

If attributes don't fit into n bits (n=1..24), they can be stored in an extra value-node with
64 bits. If this is still not enough,
a secondary data.map is used to associate a node/edge with any label.

=== Edge Attributes depending on ranges

Node indexes can be in ranges: If you have lets say 5 different types of nodes, you can store each
type of node in a certain range and change the meaning of the edge attribute bits depending on the
range the node index is in. Example:

Noderange 0-10: Person node,  8 bits to encode age years in an edge between each person node
                and company node.
          11-20: company node, 16 bits to encode the job position in an edge between a company node
                 an a person node.

The rest of the bits are used to enumerate the the edges with this attribute.
Eg you have encoded the job position in the edge then the rest of the bits can be used to
enumerate the people with this job position. If there are 10 edges out of 100.000 with a
certain attribute, then we can access these edges in 10n steps.

Too many ranges obviously slow down the library.
-}
module Graph.FastAccess (
    Judy, Node, Edge, EdgeAttr, RangeStart, Index, Start, End, Bits, NodeEdge,
    JGraph(..), NodeAttribute(..), EdgeAttribute(..),
    -- * Construction
    empty, fromListJudy,
    insertNodeEdgeList, insertNodeEdges, insertNodeEdge,
    updateNodeEdges, unionJ, insertNE,
    -- * Extraction
    getNodeEdges, nodeEdgesJ, nodesJ,
    -- * Working on all nodes
    mapNodeJ, mapNodeWithKeyJ,
    -- * Deletion
    deleteNodeEdgeList, deleteNodeJ, deleteEdgeJ, filterEdgesTo,
    -- * Query
    nullJ, adjacentNodesByAttr, adjacentNodesByIndex, lookupJudyNodes,
    adjacentEdgeCount, allChildEdges, allChildNodes, allChildNodesFromEdges,
    -- * Handling Labels
    nodeWithLabel, nodeWithMaybeLabel, nodeLabel,
    hasNodeAttr, attr, newNodeAttr, bitmask, invBitmask,
    buildWord64, extractFirstWord32, extractSecondWord32
  ) where

import           Data.Bits((.&.), (.|.))
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.List.NonEmpty(NonEmpty(..))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing, maybe, catMaybes, fromMaybe)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import           Foreign.Marshal.Alloc(allocaBytes)
import           Foreign.Ptr(castPtr)
import           Foreign.Storable(peek, pokeByteOff)
import           System.IO.Unsafe(unsafePerformIO)

import Debug.Trace


type Judy = J.JudyL Word32
-- ^ fast and memory efficient: <https://en.wikipedia.org/wiki/Judy_array>
type Node     = Word32
-- ^The number of nodes is limited by Judy using 64 bit keys and 32 bit needed for the edge label
type EdgeAttr = Word32
type Start    = Word32
-- ^ start index
type End      = Word32
-- ^ end index
type Index    = Word32
type NodeEdge = Word
-- ^ combination of 32 bit (node+attr) and 32 bit edge

type Edge = (Node,Node)
type RangeStart = Word32
type Bits = Int

-- | The main data structure
data JGraph nl el =
  JGraph { graph     :: Judy, -- ^ A Graph with 32 bit keys on the edge
           enumGraph :: Judy, -- ^ Enumerate the edges of the first graph, with counter at position 0.
                              --   Deletions in the first graph are not updated here (too costly)
           complexNodeLabelMap :: Maybe (Map Word32 nl), -- ^ A node attr that doesn't fit into 64bit
           complexEdgeLabelMap :: Maybe (Map (Node,Node) [el]),
             -- ^ An edge attr that doesn't fit into 64bit
           ranges :: NonEmpty (RangeStart, nl) -- ^ a nonempty list with an attribute for every range
         }

-- | Convert a complex node label to an attribute with (n<32) bits
--   How to do this depends on which nodes have to be filtered
class NodeAttribute nl where
    fastNodeAttr :: nl -> (Bits, Word32)
    -- ^Take a label and transforms it into n bits that
    -- should be unused in the trailing n bits of the 32bit index

    fastNodeEdgeAttr :: nl -> (el -> (Bits, Word32))


-- | Convert a complex edge label to an attribute with (n<32) bits
--   How to do this depends on which edges have to be filtered fast
class EdgeAttribute el where
    fastEdgeAttr :: NodeAttribute nl => JGraph nl el -> Node -> el -> (Bits, Word32)
  --   main attr of the arbitraryKeygraph
  --   e.g. unicode leaves 10 bits of the 32 bits unused, that could be used for the direction 
  -- of the edge, if its a right or left edge in a binary tree, etc.

------------------------------------------------------------------------------------------------
-- Generation / Insertion

-- | Generate two empty judy arrays and two empty data.maps for complex node and edge labels
--
-- The purpose of the range list is to give a special interpretation of edges depending on the node type.
empty :: NonEmpty (RangeStart, nl) -> IO (JGraph nl el)
empty ranges = do
  j <- J.new :: IO Judy
  mj <- J.new :: IO Judy
  return (JGraph j mj Nothing Nothing ranges)


-- | No usage of secondary Data.Map structure
-- Faster and more memory efficient but labels have to fit into less than 32 bits
fromListJudy :: (NodeAttribute nl, EdgeAttribute el) =>
                [(Edge, Maybe nl, Maybe nl, [el])] -> NonEmpty (RangeStart, nl) -> IO (JGraph nl el)
fromListJudy nodeEdges ranges = do
    jgraph <- empty ranges
    mapM (insertNodeEdges jgraph) nodeEdges
    return jgraph

-- | The main insertion function
insertNodeEdgeList :: (NodeAttribute nl, EdgeAttribute el) =>
                   JGraph nl el -> [(Edge, Maybe nl, Maybe nl, [el])] -> IO ()
insertNodeEdgeList jgraph ls = do
    mapM (insertNodeEdges jgraph) ls
    return ()

-- | Inserting several node-edges
insertNodeEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                   JGraph nl el -> (Edge, Maybe nl, Maybe nl, [el]) -> IO ()
insertNodeEdges jgraph ((n0, n1), nl0, nl1, edgeLabels) = do
    mapM (insertNodeEdge jgraph) (map (\el -> ((n0, n1), nl0, nl1, el)) edgeLabels)
    return ()


-- | Build the graph without using the secondary Data.Map graph
insertNodeEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                  JGraph nl el -> (Edge, Maybe nl, Maybe nl, el) -> IO ()
insertNodeEdge jgraph ((n0, n1), nl0, nl1, edgeLabel) = do
    -- the enumgraph has the number of adjacent edges at index 0
    let edgeCountKey = buildWord64 n0Key 0
    edgeCount <- J.lookup edgeCountKey mj
    if isNothing edgeCount then J.insert edgeCountKey 1 mj -- the first edge is added, set counter to 1
                           else J.insert edgeCountKey ((fromJust edgeCount)+1) mj -- inc counter by 1
    let enumKey = buildWord64 n0Key ((fromMaybe 0 edgeCount)+1)
    J.insert enumKey n1 mj

    -------------------------------------
    -- An edge consists of an attribute and a counter

    let edgeAttrCountKey = key 0 -- index 0 is the # of attributes
    maybeEdgeAttrCount <- J.lookup edgeAttrCountKey j
    let edgeAttrCount = if isJust maybeEdgeAttrCount then fromJust maybeEdgeAttrCount else 0
    let newValKey = key (edgeAttrCount+1)
    J.insert edgeAttrCountKey (edgeAttrCount+1) j
    J.insert newValKey n1Key j

  where
    j = graph jgraph
    mj = enumGraph jgraph
    n0Key = if isJust nl0 then nodeWithLabel n0 (fromJust nl0) else n0
    n1Key = if isJust nl1 then nodeWithLabel n1 (fromJust nl1) else n1
    key i = buildWord64 n0Key (i + (snd $ fastNodeEdgeAttr nLabel0 edgeLabel))
    nLabel0 = if isJust nl0 then fromJust nl0 else nodeLabel jgraph n0


updateNodeEdges :: NodeAttribute nl =>
                   JGraph nl el -> Node -> nl -> (EdgeAttr, Node) -> IO (JGraph nl el)
updateNodeEdges jgraph node nodeLabel (edge, targetNode) = do
    J.insert key targetNode (graph jgraph)
    return jgraph
  where
    key = buildWord64 (nodeWithLabel node nodeLabel) edge


-----------------------------------------------------------------------------------
-- Extract/ filter / Map over the complete Judy graph

-- | The nodes can be extracted from JGraph with (Map.keys (complexNodeLabelMap jgraph))
-- But if the graph is built only with functions from the FastAccess module
--  the nodeEdges have to be extracted with this function
nodeEdgesJ :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> IO [NodeEdge]
nodeEdgesJ jgraph = do
  immKeys <- J.freeze (graph jgraph)
  J.keys immKeys


-- | All nodes with their attribute
nodesJ :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> IO [Node]
nodesJ jgraph = do
  keys <- nodeEdgesJ jgraph
  return (map extractFirstWord32 keys)


-- | eg for filtering after 'nodesJ'
hasNodeAttr :: NodeAttribute nl => Node -> nl -> Bool
hasNodeAttr node nLabel = (node .&. (bitmask bits)) == w32
  where (bits, w32) = fastNodeAttr nLabel


-- | Extract attribute of node
--   Using the fact that all nodes have the same number of bits for attributes and a graph needs
--   to have at least one range
attr :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Node -> Word32
attr jgraph node = node .&. (bitmask bits)
  where (bits, w32) = fastNodeAttr (snd (NonEmpty.head (ranges jgraph)))


-- | Change the node attribute of a node-edge
newNodeAttr :: Bits -> (Word32 -> Word32) -> Word -> Word
newNodeAttr bits f nodeEdge = buildWord64 newNode edge
  where node = extractFirstWord32 nodeEdge
        edge = extractSecondWord32 nodeEdge
        invBm = invBitmask bits
        bm = bitmask bits
        newNode = (node .&. (invBitmask bits)) .|. (f (node .&. (bitmask bits)))

-- |@
--   e.g. bitmask 4  = 11110000 00000000 00000000 00000000 (binary)
--        bitmask 10 = 11111111 11000000 00000000 00000000 (binary)
-- @
bitmask :: Bits -> Word32
bitmask bits = (2 ^ bits) - 1  -- (2^32 - 2^(32-bits))

-- |@
--    e.g. invBitmask 4  = 00000000 00000000 00000000 00001111 (binary)
--         invBitmask 10 = 00000000 00000000 00000011 11111111 (binary)
-- @
invBitmask :: Bits -> Word32
invBitmask bits = (2^32 - 2^bits)


-- | Map a function (Word32 -> Word32) over all nodes that keeps the node index but 
--   changes the node attribute
mapNodeJ :: (NodeAttribute nl, EdgeAttribute el) =>
            (Word32 -> Word32) -> JGraph nl el -> IO (JGraph nl el)
mapNodeJ f jgraph = do
    ns <- nodeEdgesJ jgraph
    nValues <- mapM (\n -> J.lookup n j) ns
    let nodeValues = map (fromMaybe 0) nValues
    deleteNodeEdgeList jgraph ns
    let newNs = map (newNodeAttr bits f) ns
    mapM (\(key,value) -> J.insert key value j) (zip newNs nodeValues)
    return jgraph
  where
    j = graph jgraph
    (bits, _) = fastNodeAttr (snd (NonEmpty.head (ranges jgraph)))


-- | Map a function (Node -> Word32 -> Word32) over all nodes that keeps the node index but
-- changes the node attribute
mapNodeWithKeyJ :: (NodeAttribute nl, EdgeAttribute el) =>
                   (Node -> Word32 -> Word32) -> JGraph nl el -> IO (JGraph nl el)
mapNodeWithKeyJ f jgraph = do
    ns <- nodeEdgesJ jgraph
    nValues <- mapM (\n -> J.lookup n j) ns
    let nodeValues = map (fromMaybe 0) nValues
    deleteNodeEdgeList jgraph ns
    let newNode n = newNodeAttr bits (f (extractFirstWord32 n)) n
    let newNs = map newNode ns
    mapM (\(key,value) -> J.insert key value j) (zip newNs nodeValues)
    return jgraph
  where
    j = graph jgraph
    (bits, _) = fastNodeAttr (snd (NonEmpty.head (ranges jgraph)))

------------------------------------------------------------------------------------------
-- | A node-edge is deleted by deleting the key in the judy array.
--   Deleted edges that are pointed on by the enumgraph (the second judy array) are 
--   (planned!) lookup failures in the enumgraph. This is easier than some kind of garbage collection.
--
--   If you delete node or edges a lot you should currently not use this library or rebuild the
--   graph regularly.
deleteNodeEdgeList :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> [Word] -> IO ()
deleteNodeEdgeList jgraph ns = do
    mapM (\n -> J.delete n j) ns
    return ()
  where
    j = graph jgraph

-- | deletes all node-edges that contain this node, because the judy array only stores node-edges
deleteNodeJ :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Node -> IO ()
deleteNodeJ jgraph node = do
    es <- allChildEdges jgraph node
    let nodeEdges = map (buildWord64 node) es
    deleteNodeEdgeList jgraph nodeEdges

-- | "deleteEdgeJ jgraph n0 n1" deletes the edge that points from n0 to n1
--
--   It is slow because it uses 'filterEdgesTo'. If possible use 'deleteNodeEdgeList'.
deleteEdgeJ :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Node -> Node -> IO ()
deleteEdgeJ jgraph n0 n1 = do
    es <- allChildEdges jgraph n0
    let nodeEdges = map (buildWord64 n0) es
    edgesToN1 <- filterEdgesTo jgraph nodeEdges (== n1)
    deleteNodeEdgeList jgraph edgesToN1


-----------------------------------------------------------------------------------------------

-- | Find the bigger one of two judy arrays and insert all (key,value)-pairs from the smaller
-- judy array into the bigger judy array
unionJ :: (Judy, Judy) -> (Judy, Judy) -> IO (Judy, Judy)
unionJ (j0, enumJ0) (j1, enumJ1) = do
    ((biggerJ,biggerJEnum), (smallerJ, smallerJEnum)) <- biggerSmaller (j0,enumJ0) (j1,enumJ1)
    nodeEs   <- getNodeEdges smallerJ
    nodeEsMj <- getNodeEdges smallerJEnum
    insertNE nodeEs   biggerJ
    insertNE nodeEsMj biggerJEnum
    return (biggerJ,biggerJEnum)
  where
    biggerSmaller :: (Judy,Judy) -> (Judy,Judy) -> IO ((Judy,Judy), (Judy,Judy))
    biggerSmaller (g0,e0) (g1,e1) = do
       s0 <- J.size g0
       s1 <- J.size g1
       if s0 >= s1 then return ((g0,e0),(g1,e1)) else return ((g1,e1),(g0,e0))

-- | Used by unionJ
getNodeEdges :: Judy -> IO [(NodeEdge, Node)]
getNodeEdges j = do
    immKeys <- J.freeze j
    keys <- J.keys immKeys
    vs <- mapM (\k -> J.lookup k j) keys
    let values = map (fromMaybe 0) vs
    return (zip keys values)

-- TODO test overlap
-- | Used by unionJ
insertNE :: [(NodeEdge, Node)] -> Judy -> IO ()
insertNE nodeEdges j = do
    mapM (\(key,value) -> J.insert key value j) nodeEdges
    return ()


----------------------------------------------------------------------------------
-- Query

-- | Are the judy arrays of the graph both empty?
nullJ :: JGraph nl el -> IO Bool
nullJ (JGraph graph enumGraph complexNodeLabelMap complexEdgeLabelMap _) = do
  g <- J.null graph
  e <- J.null enumGraph
  return (g && e)

-- | The function that is the purpose of the whole library.
--
-- Because all edges with the same attribute bits can be enumerated, all adjacent nodes with the same
-- attribute can be retrieved with a calculated index and a judy lookup until the last judy lookup fails.
-- Eg you have 100.000 edges and 10 edges with a certain attribute, there are only 11 lookups
-- instead of 100.000 with other libraries.
adjacentNodesByAttr :: (NodeAttribute nl, EdgeAttribute el) =>
                       JGraph nl el -> Node -> el -> IO [Node]
adjacentNodesByAttr jgraph node el = do
    n <- J.lookup key j
    if isJust n then lookupJudyNodes j node attr 1 (fromJust n)
                else return []
  where
    nl = nodeLabel jgraph node
    (bits, attr) = fastNodeEdgeAttr nl el
    key = buildWord64 node attr
    j = graph jgraph


-- | Useful if you want all adjacent edges, but you cannout check all 32 bit words with a lookup
-- and the distribution of the fast attributes follows no rule, there is no other choice but to 
-- enumerate all edges. Eg you reserve 24 bit for a unicode char, but only 50 chars are used.
-- But some algorithms need all adjacent edges, for example merging results of leaf nodes in a tree
-- recursively until the root is reached
adjacentNodesByIndex :: JGraph nl el -> Node -> (Start, End) -> IO [Node]
adjacentNodesByIndex jgraph node (start, end) = do
    val <- J.lookup key mj
    if isJust val then lookupJudyNodes mj node 0 start end
                  else return []
  where
    key = buildWord64 node 0
    mj = enumGraph jgraph

-- | Recursively increases the index and tries to read a node at
--   edgeAttr + index
lookupJudyNodes :: Judy -> Node -> EdgeAttr -> Index -> End -> IO [Node]
lookupJudyNodes j node el i n = do
    val <- J.lookup key j
    next <- if i <= n then lookupJudyNodes j node el (i+1) n else return []
    return (if isJust val then ((fromJust val) : next) else next)
  where
    key = buildWord64 node (el + i)

-- | The Judy array maps a NodeEdge to a target node
--
--   Keep those NodeEdges where target node has a property (Word32 -> Bool)
filterEdgesTo :: (NodeAttribute nl, EdgeAttribute el) =>
                 JGraph nl el -> [NodeEdge] -> (Word32 -> Bool) -> IO [NodeEdge]
filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> J.lookup n j) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
  where j = graph jgraph
        filterNode (ne, Just v) | f v = True
                                | otherwise = False
        filterNode _ = False

---------------------------------------------------------------------------
-- | The number of adjacent edges
adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Node -> IO Word32
adjacentEdgeCount jgraph node = do
    let edgeCountKey = buildWord64 (nodeWithMaybeLabel node nl) 0 --the first index lookup is the count
    edgeCount <- J.lookup edgeCountKey mj
    if isJust edgeCount then return (fromJust edgeCount) else return 0
  where
    nl = maybe Nothing (Map.lookup node) (complexNodeLabelMap jgraph)
    mj = enumGraph jgraph


-- | The enumGraph enumerates all child edges
-- and maps to the second 32 bit of the key of all nodeEdges
allChildEdges :: JGraph nl el -> Node -> IO [EdgeAttr]
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
allChildNodes :: JGraph nl el -> Node -> IO [Node]
allChildNodes jgraph node = do
    edges <- allChildEdges jgraph node
    allChildNodesFromEdges jgraph edges node


-- | To avoid the recalculation of edges
allChildNodesFromEdges :: JGraph nl el -> [EdgeAttr] -> Node -> IO [Node]
allChildNodesFromEdges jgraph edges node = do
    let keys = map (buildWord64 node) edges
    nodes <- mapM (\key -> J.lookup key j) keys
    return (-- Debug.Trace.trace ("ec " ++ show enumKeys) $
            catMaybes nodes)
  where
    j = graph jgraph

----------------------------------------------------------------------------------------
-- Handling labels

nodeWithLabel :: NodeAttribute nl => Node -> nl -> Word32
nodeWithLabel node nl = node .|. (snd (fastNodeAttr nl))


nodeWithMaybeLabel :: NodeAttribute nl => Node -> Maybe nl -> Word32
nodeWithMaybeLabel node Nothing = node
nodeWithMaybeLabel node (Just nl) = node .|. (snd (fastNodeAttr nl))


-- | Every node is in a range.
--
--   Because every range has a standard label, return the label
--   that belongs to the node
nodeLabel :: NodeAttribute nl => JGraph nl el -> Node -> nl
nodeLabel jgraph node = nl (ranges jgraph)
  where nl rs | (length rs >= 2) &&
                node >= (fst (NonEmpty.head rs)) &&
                node < (fst (head (NonEmpty.tail rs))) = snd (NonEmpty.head rs)
              -- ((range0,label0):(range1,label1):rs)
              | (length rs == 1) = snd (NonEmpty.head rs)
              | otherwise = nl rs


{-# INLINE buildWord64 #-}
buildWord64 :: Word32 -> Word32 -> Word
buildWord64 w0 w1
    = unsafePerformIO . allocaBytes 8 $ \p -> do
        pokeByteOff p 0 w0
        pokeByteOff p 4 w1
        peek (castPtr p)


{-# INLINE extractFirstWord32 #-}
extractFirstWord32 :: Word -> Word32
extractFirstWord32 w
    = unsafePerformIO . allocaBytes 4 $ \p -> do
        pokeByteOff p 0 w -- 4?
        (peek (castPtr p))


{-# INLINE extractSecondWord32 #-}
extractSecondWord32 :: Word -> Word32
extractSecondWord32 w
    = unsafePerformIO . allocaBytes 4 $ \p -> do
        pokeByteOff p 4 w
        (peek (castPtr p))

