{-# LANGUAGE OverloadedStrings, FlexibleContexts, DeriveAnyClass, Strict, 
    StrictData, MultiParamTypeClasses, FlexibleInstances, InstanceSigs #-}
{-|
Module      :  JudyGraph.FastAccess
Copyright   :  (C) 2017-2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

This module only contains functions that don't use the secondary data.map structures.
It should be memory efficient and fast.

A graph consists of nodes and edges, that point from one node to another.
To encode this with a key-value storage (Judy Arrays), we encode the key as
a (node,edge)-pair and the value as a node:

@
       key           -->   value

    node | edge            node
  ---------------    -->   ------
  32 bit | 32 bit          32 bit
@

To check attributes of edges and nodes fast, we allow to use some of the node/edge bits for 
these attributes. This is called fastAttr and can be up to 32 bits. The number of 
fastAttr bits in the node has to be constant, because you are either wasting space or 
overflowing. It depends on your usage scenario if you plan to use 4 bilion nodes (2^32) 
or if you need to check attributes of a lot of nodes/edges quickly and for example 2^24 
nodes are enough, having 8 bits to encode an attribute:

@
    node | node attr |  edge  | edge attr           node
  ----------------------------------------   -->   ------
  24 bit |  8 bit    | 16 bit | 16 bit             32 bit
@

If attributes don't fit into n bits (n=1..24), they can be stored in an extra value-node 
with 64 bits. If this is still not enough,
a secondary data.map is used to associate a node/edge with any label.

=== Edge Attributes depending on ranges

Node indexes can be in ranges: If you have lets say 5 different types of nodes, you can
define each type of node to be in a certain index range and change the meaning of the edge
attribute bits depending on the range the node index is in. Example:

Noderange 0-10: Person node,  8 bits to encode age years in an edge between each person node
                and company node.
          11-20: company node, 16 bits to encode the job position in an edge between a 
                 company node and a person node.

The rest of the bits are used to enumerate the the edges with this attribute.
Eg you have encoded the job position in the edge then the rest of the bits can be used to
enumerate the people with this job position. If there are 10 edges out of 100.000 with a
certain attribute, then we can access these edges in 10n steps.

Too many ranges obviously slow down the library.
-}
module JudyGraph.FastAccess (
    GraphClass(..), NodeAttribute(..), EdgeAttribute(..), JGraph(..), Judy,
    Edge, Node32(..), Edge32(..), NodeEdge, RangeStart, Index, Start, End, Bits(..),
    AddCSVLine(..),
    -- * Construction
    emptyJ, fromListJ,
    updateNodeEdges, insertNE, mapNodeJ, mapNodeWithKeyJ,
    -- * Extraction
    getNodeEdges, nodeEdgesJ, nodesJ,
    -- * Deletion
    deleteNodeEdgeListJ,
    -- * Query
    adjacentNodesByAttr, adjacentNodeByAttr, lookupJudyNodes, lookupNodeEdge,
    -- * Handling Labels
    nodeWithLabel, nodeWithMaybeLabel, nodeLabel,
    hasNodeAttr, extrAttr, newNodeAttr, bitmask, invBitmask,
    buildWord64, extractFirstWord32, extractSecondWord32, edgeForward,
    -- * Displaying in hex for debugging
    showHex, showHex32
  ) where

import           Control.Monad
import           Data.Bits((.&.), (.|.))
import qualified Data.ByteString.Streaming as B
import qualified Data.ByteString.Char8 as C
import           Data.Char (intToDigit)
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
import           Data.Word(Word32)
import           Foreign.Marshal.Alloc(allocaBytes)
import           Foreign.Ptr(castPtr, plusPtr)
import           Foreign.Storable(peek, pokeByteOff)
import           Streaming (Of, Stream, hoist)
import           Streaming.Cassava as S
import qualified Streaming.Prelude as S
import qualified Streaming.With as S
import           System.IO.Unsafe(unsafePerformIO)
import Debug.Trace

-- ^ fast and memory efficient: <https://en.wikipedia.org/wiki/Judy_array>
type Judy = J.JudyL Word32

-- The number of nodes is limited by Judy using 64 bit keys
--  and 32 bit needed for the edge label

newtype Edge32 = Edge32 Word32
-- ^ Although both node and edge are Word32 we want to differentiate them
newtype Node32 = Node32 Word32 deriving (Eq, Ord)
-- ^ A typesafe Word32

instance Show Edge32 where show (Edge32 e) = "Edge " ++ (showHex32 e)
instance Show Node32 where show (Node32 n) = "Node " ++ (showHex32 n)

type Start    = Node32
-- ^ start index
type End      = Node32
-- ^ end index
type Index    = Node32
type NodeEdge = Word
-- ^ combination of 32 bit (node+attr) and 32 bit edge

type Edge = (Node32,Node32)
-- ^ A tuple of nodes
type RangeStart = Word32
type Bits = Int

edgeForward = 0x80000000 -- the highest bit of a 32 bit word

-- | A memory efficient way to store a graph with 64 bit key and 32 bit value
--   (node, edge) ~> node     (32 bit, 32 bit) ~> 32 bit
data (NodeAttribute nl, EdgeAttribute el) =>
     JGraph nl el = JGraph {
  judyGraphJ :: Judy, -- ^ A Graph with 32 bit keys on the edge
  rangesJ :: NonEmpty (RangeStart, nl, [el]), -- ^ a nonempty list with an attribute for
                       -- every range and assigning which edgelabels are valid in each range
  nodeCountJ :: Word32
}

----------------------------------------------------------------------------------------
-- | A general class of functions that all three graphs have to support
class GraphClass graph nl el where
  empty :: NonEmpty (RangeStart, nl, [el]) -> IO (graph nl el)
  isNull :: graph nl el -> IO Bool
  fromList :: Bool -> [(Node32, nl)] -> [(Edge, Maybe nl, Maybe nl, [el], Bool)]
                                     -> [(Edge, Maybe nl, Maybe nl, [el])]
           -> NonEmpty (RangeStart, nl, [el]) -> IO (graph nl el)
  insertNodeEdge ::  Bool -> graph nl el ->  (Edge,Maybe nl,Maybe nl,el,Bool)
                 -> IO (graph nl el)
  -- | Insert several edges using 'insertNodeEdge'
  insertNodeEdges :: Bool -> graph nl el -> [(Node32, nl)] -> [(Edge,Maybe nl,Maybe nl,[el],Bool)]
                  -> IO (graph nl el)
  insertNodeEdges overwrite jgraph nodes es = fmap (addNodeCount nodes) (foldM foldEs jgraph es)
    where
      foldEs g ((n0, n1), nl0, nl1, edgeLs, dir) = foldM (insertNodeEdge overwrite) g
                                                         (map addN edgeLs)
        where addN el = ((n0, n1), nl0, nl1, el, dir)
  addNodeCount :: [(Node32, nl)] -> graph nl el -> graph nl el
  insertNodeEdgeAttr :: Bool -> graph nl el -> (Edge,Maybe nl,Maybe nl,Edge32,Edge32)
                        -> IO (graph nl el, (Bool, (Node32,Word32)))
  insertCSVEdgeStream :: (NodeAttribute nl, EdgeAttribute el, Show el) =>
                         graph nl el -> FilePath ->
                         (graph nl el -> [String] -> IO (graph nl el)) -> IO (graph nl el)
  -- | A helper function for insertCSVEdgeStream
  insertCSVEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                   (graph nl el -> [String] -> IO (graph nl el))
                 -> graph nl el -> Either CsvParseException [String] -> IO (graph nl el)
  -- | merge two graphs into one
  union :: graph nl el -> graph nl el -> IO (graph nl el)
  deleteNode  :: graph nl el -> Node32 -> IO (graph nl el)
  deleteNodes :: graph nl el -> [Node32] -> IO (graph nl el)
  deleteEdge :: (graph nl el) -> Edge -> IO (graph nl el)
  adjacentEdgesByAttr :: graph nl el -> Node32 -> Edge32 -> IO [Edge32]
  filterEdgesTo :: graph nl el -> [NodeEdge] -> (Edge32 -> Bool) -> IO [NodeEdge]
--  allChildEdges :: graph nl el -> Node -> IO [EdgeAttr32]
--  allChildNodes :: graph nl el -> Node -> IO [Node]
--  allChildNodesFromEdges :: graph nl el -> Node -> [EdgeAttr32] -> IO [Node]

  nodeCount :: graph nl el -> Word32
  ranges :: graph nl el -> NonEmpty (RangeStart, nl, [el])
  judyGraph :: graph nl el -> Judy


-- | Convert a complex node label to an attribute with (n<32) bits
--   How to do this depends on which nodes have to be filtered
class NodeAttribute nl where
    fastNodeAttr :: nl -> (Bits, Word32)
    -- ^Take a label and transforms it into n bits that
    -- should be unused in the trailing n bits of the 32bit index

-- | Convert a complex edge label to an attribute with (n<32) bits
--   How to do this depends on which edges have to be filtered fast
class EdgeAttribute el where
    fastEdgeAttr :: el -> (Bits, Word32)
    fastEdgeAttrBase :: el -> Word32 -- The key that is used for counting
--    edgeForward :: el -> Word32 -- 0 if the edge is "in direction", otherwise a value (a bit) that 
--                      -- does not interfere with the rest of the attr (orthogonal attr)
  --   main attr of the arbitraryKeygraph
  --   e.g. unicode leaves 10 bits of the 32 bits unused, that could be used for the
  --   direction of the edge, if its a right or left edge in a binary tree, etc.

class AddCSVLine graph nl el where
  addCsvLine :: (NodeAttribute nl, Enum nl, Show nl) =>
                  Map String Word32 -- ^ A map for looking up nodes by their name
               -> graph nl el -- ^ A graph
               -> [String]    -- ^ A string for each element of the line
               -> IO (graph nl el) -- ^ The IO action that adds something to the graph

-----------------------------------------------------------------------------------------

instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
         GraphClass JGraph nl el where
  -- | Generate two empty judy arrays and two empty data.maps for complex node and edge
  --   labels. The purpose of the range list is to give a special interpretation of edges
  --   depending on the node type.
  empty ranges = do
    j <- J.new :: IO Judy
    return (JGraph j ranges 0)


  -- | Is the judy array of the graph empty?
  isNull (JGraph graph _ _) = J.null graph


  fromList overwrite nodes directedEdges nodeEdges ranges = do
    jgraph <- empty ranges
    insertNodeEdges overwrite jgraph nodes
                    (directedEdges ++ (map addDir nodeEdges) ++ (map dirRev nodeEdges) )
    where addDir ((from,to), nl0, nl1, labels) = ((from,to), nl1, nl0, labels, True)
          dirRev ((from,to), nl0, nl1, labels) = ((to,from), nl1, nl0, labels, True)

  addNodeCount nodes jgraph = jgraph { nodeCountJ = (nodeCountJ jgraph) +
                                                    (fromIntegral (length nodes)) }

  -- | Build the graph without using the secondary Data.Map graph
  --   If edge already exists and (overwrite == True) overwrite it
  --   otherwise create a new edge and increase counter (that is at index 0)
  insertNodeEdge :: Bool -> JGraph nl el -> ((Node32, Node32), Maybe nl, Maybe nl, el, Bool)
                  -> IO (JGraph nl el)
  insertNodeEdge overwrite jgraph ((n0, n1), nl0, nl1, edgeLabel, dir) =
      fmap fst $ insertNodeEdgeAttr overwrite jgraph
                                    ((n0, n1), nl0, nl1, Edge32 attr, Edge32 attrBase)
    where
      attr =   (snd (fastEdgeAttr edgeLabel)) + (if dir then 0 else edgeForward)
      attrBase = (fastEdgeAttrBase edgeLabel) + (if dir then 0 else edgeForward)


  insertNodeEdgeAttr overwrite jgraph
                    ((Node32 n0, Node32 n1), nl0, nl1, Edge32 attr, Edge32 attrBase) = do
    -- An edge consists of an attribute and a counter
    let edgeAttrCountKey = buildWord64 n0Key attrBase
    maybeEdgeAttrCount <- J.lookup edgeAttrCountKey j
    let edgeAttrCount = fromMaybe 0 maybeEdgeAttrCount

--    debugToCSV (n0Key,n1Key) edgeLabel
    -------------------------------------
    let newValKey = buildWord64 n0Key (attr + if overwrite then 0 else edgeAttrCount)
    n2 <- J.lookup newValKey j
    let isEdgeNew = isNothing n2
    when (isEdgeNew || (not overwrite)) (J.insert edgeAttrCountKey (edgeAttrCount+1) j)
    J.insert newValKey n1Key j -- (Debug.Trace.trace ("Fast"++ show (n0, n1) ++" "++ show edgeAttrCount ++" "++ showHex newValKey ++"("++ showHex32 n0Key ++","++ showHex32 n1Key ++")"++ showHex32 attr ++ show nl1 ++ showHex32 n0Key ++ showHex32 attrBase) j)
    let newN = fromMaybe (Node32 n1) (fmap Node32 n2)
    if isEdgeNew || (not overwrite)
      then return (jgraph { nodeCountJ = (nodeCount jgraph) + 1},
                           (isEdgeNew, (Node32 n1, edgeAttrCount)))
      else return (jgraph, (isEdgeNew, (newN, edgeAttrCount)))
   where
    j = judyGraph jgraph
    n0Key = maybe n0 (nodeWithLabel (Node32 n0)) nl0
    n1Key = maybe n1 (nodeWithLabel (Node32 n1)) nl1


  -- | In a dense graph the edges might be too big to be first stored in a list before being
  --   added to the judy graph. Therefore the edges are streamed from a .csv-file line by
  --   line and then added to the judy-graph. A function is passed that can take a line
  --   (a list of strings) and add it to the graph.
  insertCSVEdgeStream :: (NodeAttribute nl, EdgeAttribute el, Show el) =>
                          JGraph nl el -> FilePath ->
                         (JGraph nl el -> [String] -> IO (JGraph nl el))
                       -> IO (JGraph nl el)
  insertCSVEdgeStream graph file newEdge = do
    a <- S.withBinaryFileContents file
                            ((S.foldM (insertCSVEdge newEdge) (return graph) return) . dec)
    return (fst (S.lazily a))
   where
    dec :: B.ByteString IO () ->
           Stream (Of (Either CsvParseException [String]))
                  IO
                  (Either (CsvParseException, B.ByteString IO ()) ())
    dec = S.decodeWithErrors S.defaultDecodeOptions NoHeader


  -- | A helper function for insertCSVEdgeStream
  insertCSVEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                   (JGraph nl el -> [String] -> IO (JGraph nl el))
                 -> JGraph nl el -> Either CsvParseException [String] -> IO (JGraph nl el)
  insertCSVEdge newEdge g (Right edgeProp) = newEdge g edgeProp
  insertCSVEdge newEdge g (Left message)   = return g


  -- | Deletes all node-edges that contain this node, because the judy array only stores
  --   node-edges
  --  deleteNode :: (NodeAttribute nl, EdgeAttribute el) =>
  --                JGraph nl el -> Node -> IO (JGraph nl el)
  deleteNode jgraph (Node32 node) = do
  --    es <- allChildEdges jgraph node
    let nodeEdges = map (buildWord64 node) [] -- es
    deleteNodeEdgeListJ jgraph nodeEdges


  deleteNodes jgraph nodes = do
    newNodeMap <- foldM deleteNode jgraph nodes
    return jgraph


  -- | "deleteEdge jgraph (n0, n1)" deletes the edge that points from n0 to n1
  --
  --   It is slow because it uses 'filterEdgesTo'. If possible use 'deleteNodeEdgeList'.
  deleteEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                (JGraph nl el) -> Edge -> IO (JGraph nl el)
  deleteEdge jgraph (Node32 n0, Node32 n1) = do
    -- es <- allChildEdges jgraph n0
    let nodeEdges = map (buildWord64 n0) [] -- es
    edgesToN1 <- filterEdgesTo jgraph nodeEdges (\(Edge32 e) -> e == n1)
    deleteNodeEdgeListJ jgraph edgesToN1

  -- | Find the bigger one of two judy arrays and insert all (key,value)-pairs from the
  --   smaller judy array into the bigger judy array
  union g0 g1 = do
    ((JGraph biggerJ br bn), (JGraph smallerJ sr sn)) <- biggerSmaller g0 g1
    nodeEs <- getNodeEdges smallerJ
    insertNE nodeEs biggerJ
    return (JGraph biggerJ br bn)
   where
    biggerSmaller :: (NodeAttribute nl, EdgeAttribute el) =>
                     JGraph nl el -> JGraph nl el -> IO (JGraph nl el, JGraph nl el)
    biggerSmaller (JGraph g0 r0 n0) (JGraph g1 r1 n1) = do
       s0 <- J.size g0
       s1 <- J.size g1
       if s0 >= s1 then return ((JGraph g0 r0 n0), (JGraph g1 r1 n1))
                   else return ((JGraph g1 r1 n1), (JGraph g0 r0 n0))

  -- | Introduced for the cypher interface
  --   Makes a lookup to see how many edges there are
  -- TODO: Should they also lookup the target nodes?
  --       Currently Yes, just to make sure they exist
  adjacentEdgesByAttr jgraph (Node32 node) (Edge32 attr) = do
    n <- J.lookup key j
    map fst <$> maybe (return [])
                      (lookupJudyNodes j (Node32 node) (Edge32 attr) (Node32 1))
                      (fmap Node32 n)
-- (Debug.Trace.trace ("eAdj "++ show n ++" "++ show node ++" "++ showHex32 attr ++" "++ showHex key) n)
   where
    key = buildWord64 node attr
    j = judyGraph jgraph

  -- | The Judy array maps a NodeEdge to a target node
  --
  --   Keep those NodeEdges where target node has a property (Word32 -> Bool)
  filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> J.lookup n j) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
   where j = judyGraph jgraph
         filterNode (ne, Just v) | f (Edge32 v) = True
                                 | otherwise = False
         filterNode _ = False

  nodeCount graph = nodeCountJ graph
  ranges :: Enum nl => JGraph nl el -> NonEmpty (RangeStart, nl, [el])
  ranges graph = rangesJ graph
  judyGraph graph = judyGraphJ graph


emptyJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
          NonEmpty (RangeStart, nl, [el]) -> IO (JGraph nl el)
emptyJ rs = empty rs


fromListJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
             Bool -> [(Node32, nl)] -> [(Edge, Maybe nl, Maybe nl, [el], Bool)]
                                    -> [(Edge, Maybe nl, Maybe nl, [el])] ->
             NonEmpty (RangeStart, nl, [el]) -> IO (JGraph nl el)
fromListJ overwrite nodes dirEdges edges ranges =
  fromList overwrite nodes dirEdges edges ranges

-------------------------------------------------------------------------------------------

updateNodeEdges :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                   JGraph nl el -> Node32 -> nl -> (Edge32, Node32) -> IO (JGraph nl el)
updateNodeEdges jgraph node nodeLabel (Edge32 edge, Node32 targetNode) = do
    J.insert key targetNode (judyGraph jgraph)
    return jgraph
  where
    key = buildWord64 (nodeWithLabel node nodeLabel) edge


-----------------------------------------------------------------------------------
-- Extract/ filter / Map over the complete Judy graph

-- | The nodes can be extracted from JGraph with (Map.keys (complexNodeLabelMap jgraph))
-- But if the graph is built only with functions from the FastAccess module
--  the nodeEdges have to be extracted with this function
nodeEdgesJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
              JGraph nl el -> IO [NodeEdge]
nodeEdgesJ jgraph = do
  immKeys <- J.freeze (judyGraph jgraph)
  J.keys immKeys

targetNodesJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                JGraph nl el -> IO [Node32]
targetNodesJ jgraph = do
  imm <- J.freeze (judyGraph jgraph)
  fmap (map Node32) (J.elems imm)

-- | All nodes (with duplicates => probably useless)
nodesJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
          JGraph nl el -> IO [Node32]
nodesJ jgraph = do
  keys <- nodeEdgesJ jgraph
  values <- targetNodesJ jgraph
  return ((map (Node32 . extractFirstWord32) keys) ++ values)


-- | eg for filtering after 'nodesJ'
hasNodeAttr :: NodeAttribute nl => Node32 -> nl -> Bool
hasNodeAttr (Node32 node) nLabel = (node .&. bitmask bits) == w32
  where (bits, w32) = fastNodeAttr nLabel


-- | Extract attribute of node
--   Using the fact that all nodes have the same number of bits for attributes and a graph
--   needs to have at least one range
extrAttr :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
            JGraph nl el -> Node32 -> Word32
extrAttr jgraph (Node32 node) = node .&. bitmask bits
  where (bits, w32) = fastNodeAttr (sec (NonEmpty.head (ranges jgraph)))
        sec (x,y,z) = y

-- | Change the node attribute of a node-edge
newNodeAttr :: Bits -> (Word32 -> Word32) -> Word -> Word
newNodeAttr bits f nodeEdge = buildWord64 newNode edge
  where node = extractFirstWord32 nodeEdge
        edge = extractSecondWord32 nodeEdge
        invBm = invBitmask bits
        bm = bitmask bits
        newNode = (node .&. invBitmask bits) .|. f (node .&. bitmask bits)

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
invBitmask bits = 2^32 - 2^bits


-- | Map a function (Word32 -> Word32) over all nodes that keeps the node index but 
--   changes the node attribute
mapNodeJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
            (Word32 -> Word32) -> JGraph nl el -> IO (JGraph nl el)
mapNodeJ f jgraph = do
    ns <- nodeEdgesJ jgraph
    nValues <- mapM (\n -> J.lookup n j) ns
    let nodeValues = map (fromMaybe 0) nValues
    deleteNodeEdgeListJ jgraph ns
    let newNs = map (newNodeAttr bits f) ns
    mapM_ (\(key,value) -> J.insert key value j) (zip newNs nodeValues)
    return jgraph
  where
    j = judyGraph jgraph
    (bits, _) = fastNodeAttr (sec (NonEmpty.head (ranges jgraph)))
    sec (x,y,z) = y

-- | Map a function (Node -> Word32 -> Word32) over all nodes that keeps the node index but
-- changes the node attribute
mapNodeWithKeyJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                   (Node32 -> Word32 -> Word32) -> JGraph nl el -> IO (JGraph nl el)
mapNodeWithKeyJ f jgraph = do
    ns <- nodeEdgesJ jgraph
    nValues <- mapM (\n -> J.lookup n j) ns
    let nodeValues = map (fromMaybe 0) nValues
    deleteNodeEdgeListJ jgraph ns
    let newNode n = newNodeAttr bits (f (Node32 (extractFirstWord32 n))) n
    let newNs = map newNode ns
    mapM_ (\(key,value) -> J.insert key value j) (zip newNs nodeValues)
    return jgraph
  where
    j = judyGraph jgraph
    (bits, _) = fastNodeAttr (sec (NonEmpty.head (ranges jgraph)))
    sec (x,y,z) = y

------------------------------------------------------------------------------------------
-- | A node-edge is deleted by deleting the key in the judy array.
--   Deleted edges that are pointed on by the enumgraph (the second judy array) are 
--   (planned!) lookup failures in the enumgraph. This is easier than some kind of
--   garbage collection.
--
--   If you delete node or edges a lot you should currently not use this library or rebuild
--   the graph regularly.
deleteNodeEdgeListJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                      JGraph nl el -> [Word] -> IO (JGraph nl el)
deleteNodeEdgeListJ jgraph ns = do
    mapM_ (\n -> J.delete n (judyGraph jgraph)) ns
    return jgraph -- TODO counter?


-------------------------------------------------------------------------------------------

-- | Used by union
getNodeEdges :: Judy -> IO [(NodeEdge, Node32)]
getNodeEdges j = do
    immKeys <- J.freeze j
    keys <- J.keys immKeys
    vs <- mapM (\k -> J.lookup k j) keys
    let values = map (Node32 . fromMaybe 0) vs
    return (zip keys values)

-- TODO test overlap
-- | Used by unionJ
insertNE :: [(NodeEdge, Node32)] -> Judy -> IO ()
insertNE nodeEdges j = do
    mapM_ (\(key, Node32 value) -> J.insert key value j) nodeEdges
    return ()


----------------------------------------------------------------------------------------
-- Query

-- | Return a single node
adjacentNodeByAttr :: (GraphClass graph nl el,
                       NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                       graph nl el -> Node32 -> el -> IO (Maybe Node32)
adjacentNodeByAttr jgraph (Node32 node) el =
    fmap (fmap Node32) (J.lookup key j)
  where
--    nl = Debug.Trace.trace "nl " $ nodeLabel jgraph node -- outcomment and you will get an endless loop!?
    (bits, attr) = fastEdgeAttr el
    key = -- Debug.Trace.trace ("adjacentNodeByAttr "++ show node ++" "++ showHex32 attr
          --                    ++" "++ showHex (buildWord64 node attr)) $
          buildWord64 node attr
    j = judyGraph jgraph


-- | The function that is the purpose of the whole library.
--
-- Because all edges with the same attribute bits can be enumerated, n adjacent nodes with
-- the same attribute can be retrieved with a calculated index and a judy lookup.
-- Eg you have 100.000 edges and 10 edges with a certain attribute, there are only 11
-- lookups instead of 100.000 with other libraries.
adjacentNodesByAttr :: (GraphClass graph nl el,
                        NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                       graph nl el -> Node32 -> el -> IO [(Edge32, Node32)]
adjacentNodesByAttr jgraph (Node32 node) el = do
    n <- J.lookup key j
    maybe (return [])
          (lookupJudyNodes j (Node32 node) (Edge32 attr) (Node32 1))
          (fmap Node32 n)
-- (Debug.Trace.trace ("valAdj "++ show n ++" "++ show node ++" "++ showHex key ++" "++ showHex32 attr) n)
  where
    attr = fastEdgeAttrBase el
    key = buildWord64 node attr
    j = judyGraph jgraph


-- | Recursively increases the index and tries to read a node at
--   edgeAttr + index
lookupJudyNodes :: Judy -> Node32 -> Edge32 -> Index -> End -> IO [(Edge32, Node32)]
lookupJudyNodes j (Node32 node) (Edge32 attr) (Node32 i) (Node32 n) = do
    val <- J.lookup key j
    next <- if i <= n -- (Debug.Trace.trace ("lJ "++ showHex32 node ++" "++ showHex32 (attr+i) ++" "++ show val) n)
            then lookupJudyNodes j (Node32 node) (Edge32 attr) (Node32 (i+1)) (Node32 n)
            else return []
    return (if isJust val then (Edge32 (attr + i), Node32 (fromJust val)) : next
                          else next)
  where
    key = buildWord64 node (attr + i)


-- | Return a single node
lookupNodeEdge :: Judy -> Node32 -> Edge32 -> IO (Maybe Node32)
lookupNodeEdge j (Node32 node) (Edge32 edge) = fmap (fmap Node32) (J.lookup key j)
  where
    key = buildWord64 node edge

-------------------------------------------------------------------------
-- Handling labels

nodeWithLabel :: NodeAttribute nl => Node32 -> nl -> Word32
nodeWithLabel (Node32 node) nl = node .|. (snd (fastNodeAttr nl))


nodeWithMaybeLabel :: NodeAttribute nl => Node32 -> Maybe nl -> Word32
nodeWithMaybeLabel (Node32 node) Nothing = node
nodeWithMaybeLabel (Node32 node) (Just nl) = node .|. (snd (fastNodeAttr nl))


-- | Every node is in a range.
--
--   Because every range has a standard label, return the label
--   that belongs to the node
nodeLabel ::
  (GraphClass graph nl el, NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
  graph nl el -> Node32 -> nl
nodeLabel jgraph (Node32 node) = nl (ranges jgraph)
  where nl rs | (length rs >= 2) &&
                node >= first (NonEmpty.head rs) &&
                node < first (head (NonEmpty.tail rs)) = sec (NonEmpty.head rs)
              -- ((range0,label0):(range1,label1):rs)
              | length rs == 1 = sec (NonEmpty.head rs)
              | otherwise = nl rs
        first (x,y,z) = x
        sec (x,y,z) = y

-- | concatenate two Word32 to a Word (64 bit)
{-# INLINE buildWord64 #-}
buildWord64 :: Word32 -> Word32 -> Word
buildWord64 w0 w1
    = unsafePerformIO . allocaBytes 8 $ \p -> do
        pokeByteOff p 0 w0
        pokeByteOff p 4 w1
        peek (castPtr p)

-- | extract the first 32 bit of a 64 bit word
{-# INLINE extractFirstWord32 #-}
extractFirstWord32 :: Word -> Word32
extractFirstWord32 w
    = unsafePerformIO . allocaBytes 4 $ \p -> do
        pokeByteOff p 0 w
        peek (castPtr p)

-- | extract the second 32 bit of a 64 bit word
{-# INLINE extractSecondWord32 #-}
extractSecondWord32 :: Word -> Word32
extractSecondWord32 w
    = unsafePerformIO . allocaBytes 4 $ \p -> do
        pokeByteOff p 0 w
        peek (castPtr (plusPtr p 4))

------------------------------------------------------------------
-- Debugging

-- | display a 64 bit word so that we can see the bits better
showHex :: Word -> String
showHex n = showIt 16 n ""
   where
    showIt :: Int -> Word -> String -> String
    showIt 0 _ r = r
    showIt i x r = case quotRem x 16 of
                       (y, z) -> let c = intToDigit (fromIntegral z)
                                 in c `seq` showIt (i-1) y (c:r)

-- | display a 32 bit word so that we can see the bits better
showHex32 :: Word32 -> String
showHex32 n = showIt 8 n ""
   where
    showIt :: Int -> Word32 -> String -> String
    showIt 0 _ r = r
    showIt i x r = case quotRem x 16 of
                       (y, z) -> let c = intToDigit (fromIntegral z)
                                 in c `seq` showIt (i-1) y (c:r)


-- | Generate a file that can be displayed for debugging
debugToCSV :: (EdgeAttribute el, Show el) => Edge -> el -> IO ()
debugToCSV (n0,n1) edgeLabel =
  do Text.appendFile "ghc-core-graph/csv/debugNodes.csv"
                     (Text.pack (show n0 ++ "\n" ++ show n1 ++ "\n"))
     Text.appendFile "ghc-core-graph/csv/debugEdges.csv"
                     (Text.pack (show n0 ++","++ show n1 ++","++ show edgeLabel ++"\n"))

