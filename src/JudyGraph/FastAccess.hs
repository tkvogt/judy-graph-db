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

Node indexes can be in ranges: If you have lets say 5 different types of nodes, you can
define each type of node to be in a certain index range and change the meaning of the edge
attribute bits depending on the range the node index is in. Example:

Noderange 0-10: Person node,  8 bits to encode age years in an edge between each person node
                and company node.
          11-20: company node, 16 bits to encode the job position in an edge between a company
                 node and a person node.

The rest of the bits are used to enumerate the the edges with this attribute.
Eg you have encoded the job position in the edge then the rest of the bits can be used to
enumerate the people with this job position. If there are 10 edges out of 100.000 with a
certain attribute, then we can access these edges in 10n steps.

Too many ranges obviously slow down the library.
-}
module JudyGraph.FastAccess (
    GraphClass(..), NodeAttribute(..), EdgeAttribute(..), JGraph(..), Judy,
    Node, Edge, NodeEdge, EdgeAttr32, RangeStart, Index, Start, End, Bits(..),
    -- * Construction
    emptyJ, fromListJ, insertCSVEdgeStream,
    updateNodeEdges, insertNE, mapNodeJ, mapNodeWithKeyJ,
    -- * Extraction
    getNodeEdges, nodeEdgesJ, nodesJ,
    -- * Deletion
    deleteNodeEdgeListJ,
    -- * Query
    adjacentNodesByAttr, adjacentNodeByAttr, lookupJudyNodes,
    -- * Handling Labels
    nodeWithLabel, nodeWithMaybeLabel, nodeLabel,
    hasNodeAttr, extrAttr, newNodeAttr, bitmask, invBitmask,
    buildWord64, extractFirstWord32, extractSecondWord32,
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


type Judy = J.JudyL Word32
-- ^ fast and memory efficient: <https://en.wikipedia.org/wiki/Judy_array>
type Node     = Word32
-- ^The number of nodes is limited by Judy using 64 bit keys and 32 bit needed for the edge label
type EdgeAttr32 = Word32
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

-- | A memory efficient way to store a graph with 64 bit key and 32 bit value
--   (node, edge) ~> node     (32 bit, 32 bit) ~> 32 bit
data (NodeAttribute nl, EdgeAttribute el) =>
     JGraph nl el = JGraph {
  judyGraphJ :: Judy, -- ^ A Graph with 32 bit keys on the edge
  -- ^ An edge attr that doesn't fit into 64bit
  rangesJ :: NonEmpty (RangeStart, nl), -- ^ a nonempty list with an attribute for every range
  nodeCountJ :: Word32
}

----------------------------------------------------------------------------------------
-- classes

class GraphClass graph nl el where
  empty :: NonEmpty (RangeStart, nl) -> IO (graph nl el)
  isNull :: graph nl el -> IO Bool
  fromList :: Bool -> [(Node, nl)] -> [(Edge, Maybe nl, Maybe nl, [el])]
                                   -> [(Edge, Maybe nl, Maybe nl, [el])] ->
              NonEmpty (RangeStart, nl) -> IO (graph nl el)
  insertNodeEdge ::  Bool -> graph nl el ->  (Edge, Maybe nl, Maybe nl, el)    -> IO (graph nl el)
  -- | Insert several edges using 'insertNodeEdge'
  insertNodeEdges :: Bool -> graph nl el -> [(Edge, Maybe nl, Maybe nl, [el])] -> IO (graph nl el)
  insertNodeEdges overwrite jgraph es = foldM foldEs jgraph es
    where
      foldEs g ((n0, n1), nl0, nl1, edgeLs) = foldM (insertNodeEdge overwrite) g (map addN edgeLs)
        where addN el = ((n0, n1), nl0, nl1, el)
  insertNodeEdgeAttr :: Bool -> graph nl el -> (Edge, Maybe nl, Maybe nl, EdgeAttr32, EdgeAttr32)
                        -> IO (graph nl el, (Bool, Node))

  union :: graph nl el -> graph nl el -> IO (graph nl el)
  deleteNode  :: graph nl el -> Node -> IO (graph nl el)
  deleteNodes :: graph nl el -> [Node] -> IO (graph nl el)
  deleteEdge :: (graph nl el) -> Edge -> IO (graph nl el)
  adjacentEdgesByAttr :: graph nl el -> Node -> EdgeAttr32 -> IO [EdgeAttr32]
  filterEdgesTo :: graph nl el -> [NodeEdge] -> (Word32 -> Bool) -> IO [NodeEdge]
  allChildEdges :: graph nl el -> Node -> IO [EdgeAttr32]
  allChildNodes :: graph nl el -> Node -> IO [Node]
  allChildNodesFromEdges :: graph nl el -> Node -> [EdgeAttr32] -> IO [Node]

  nodeCount :: graph nl el -> Word32
  ranges :: graph nl el -> NonEmpty (RangeStart, nl)
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
    edgeForward :: Maybe el -- edge Label for edge in direction given (orthogonal attr)
  --   main attr of the arbitraryKeygraph
  --   e.g. unicode leaves 10 bits of the 32 bits unused, that could be used for the direction 
  -- of the edge, if its a right or left edge in a binary tree, etc.

    addCsvLine :: (NodeAttribute nl, Show nl) =>
                  Map String Word32 -- ^ A map for looking up nodes by their name
              -> JGraph nl el -- ^ A graph
              -> [String]    -- ^ A string for each element of the line
              -> IO (JGraph nl el) -- ^ The IO action that adds something to the graph

------------------------------------------------------------------------------------------------

instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
         GraphClass JGraph nl el where
  -- | Generate two empty judy arrays and two empty data.maps for complex node and edge labels
  --
  -- The purpose of the range list is to give a special interpretation of edges
  -- depending on the node type.
  empty ranges = do
    j <- J.new :: IO Judy
    return (JGraph j ranges 0)


  -- | Is the judy array of the graph empty?
  isNull (JGraph graph _ _) = J.null graph


  fromList overwrite nodes directedEdges nodeEdges ranges = do
    jgraph <- empty ranges
    insertNodeEdges overwrite jgraph (directedEdges ++ nodeEdges)

  -- | Build the graph without using the secondary Data.Map graph
  --   If edge already exists and (overwrite == True) overwrite it
  --   otherwise create a new edge and increase counter (that is at index 0)
  insertNodeEdge overwrite jgraph ((n0, n1), nl0, nl1, edgeLabel) =
      fmap fst $ insertNodeEdgeAttr overwrite jgraph ((n0, n1), nl0, nl1, edgeAttr, edgeAttrBase)
    where
      edgeAttr = snd (fastEdgeAttr edgeLabel)
      edgeAttrBase = fastEdgeAttrBase edgeLabel


  insertNodeEdgeAttr overwrite jgraph ((n0, n1), nl0, nl1, edgeAttr, edgeAttrBase) = do
    -- An edge consists of an attribute and a counter
    let edgeAttrCountKey = buildWord64 n0Key edgeAttrBase
    maybeEdgeAttrCount <- J.lookup edgeAttrCountKey j
    let edgeAttrCount = fromMaybe 0 maybeEdgeAttrCount

--    debugToCSV (n0Key,n1Key) edgeLabel
    -------------------------------------
    let newValKey = buildWord64 n0Key (edgeAttr + if overwrite then 0 else edgeAttrCount)
    n2 <- J.lookup newValKey j
    let isEdgeNew = isNothing n2
    when (isEdgeNew || (not overwrite)) (J.insert edgeAttrCountKey (edgeAttrCount+1) j)
    J.insert newValKey n1Key j -- (Debug.Trace.trace (show (n0, n1) ++" "++ show edgeAttrCount ++" "++ showHex newValKey ++"("++ showHex32 n0Key ++","++ showHex32 n1Key ++")"++ showHex32 edgeAttr ++ show nl1) j)
    if isEdgeNew || (not overwrite)
      then return (jgraph { nodeCountJ = (nodeCount jgraph) + 1}, (isEdgeNew, n1))
      else return (jgraph, (isEdgeNew, fromMaybe n1 n2))
   where
    j = judyGraph jgraph
    n0Key = maybe n0 (nodeWithLabel n0) nl0
    n1Key = maybe n1 (nodeWithLabel n1) nl1


  -- | deletes all node-edges that contain this node, because the judy array only stores node-edges
  --  deleteNode :: (NodeAttribute nl, EdgeAttribute el) =>
  --                JGraph nl el -> Node -> IO (JGraph nl el)
  deleteNode jgraph node = do
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
  deleteEdge jgraph (n0, n1) = do
    -- es <- allChildEdges jgraph n0
    let nodeEdges = map (buildWord64 n0) [] -- es
    edgesToN1 <- filterEdgesTo jgraph nodeEdges (== n1)
    deleteNodeEdgeListJ jgraph edgesToN1

  -- | Find the bigger one of two judy arrays and insert all (key,value)-pairs from the smaller
  -- judy array into the bigger judy array
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
  adjacentEdgesByAttr jgraph node attr = do
    n <- J.lookup key j
    map fst <$> maybe (return []) (lookupJudyNodes j node attr 1) n -- (Debug.Trace.trace ("eAdj "++ show n ++" "++ show node ++" "++ showHex32 attr ++" "++ showHex key) n)
   where
    key = buildWord64 node attr
    j = judyGraph jgraph


  allChildEdges jgraph node = do
    return []

  allChildNodes jgraph node = do
    return []

  allChildNodesFromEdges jgraph node edges = do
    return []

  -- | The Judy array maps a NodeEdge to a target node
  --
  --   Keep those NodeEdges where target node has a property (Word32 -> Bool)
  filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> J.lookup n j) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
   where j = judyGraph jgraph
         filterNode (ne, Just v) | f v = True
                                 | otherwise = False
         filterNode _ = False

  nodeCount graph = nodeCountJ graph
  ranges :: Enum nl => JGraph nl el -> NonEmpty (RangeStart, nl)
  ranges graph = rangesJ graph
  judyGraph graph = judyGraphJ graph


emptyJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
          NonEmpty (RangeStart, nl) -> IO (JGraph nl el)
emptyJ rs = empty rs


fromListJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
             Bool -> [(Node, nl)] -> [(Edge, [el])]-> [(Edge, [el])] ->
             NonEmpty (RangeStart, nl) -> IO (JGraph nl el)
fromListJ overwrite nodes dirEdges edges ranges =
  fromList overwrite nodes (addN dirEdges) (addN edges) ranges
    where addN = map (\(ns, es) -> (ns, Nothing, Nothing, es))

------------------------------------------------------------------------------------------------

-- | In a dense graph the edges might be too big to be first stored in a list before being
--   added to the judy graph. Therefore the edges are streamed from a .csv-file line by line and 
--   then added to the judy-graph. A function is passed that can take a line (a list of strings)
--   and add it to the graph.
insertCSVEdgeStream :: (NodeAttribute nl, EdgeAttribute el, Show el) =>
                       JGraph nl el -> FilePath ->
                       (JGraph nl el -> [String] -> IO (JGraph nl el)) -> IO (JGraph nl el)
insertCSVEdgeStream jgraph file newEdge = do
  a <- S.withBinaryFileContents file
                            ((S.foldM (insertCSVEdge newEdge) (return jgraph) return) . dec)
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


updateNodeEdges :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                   JGraph nl el -> Node -> nl -> (EdgeAttr32, Node) -> IO (JGraph nl el)
updateNodeEdges jgraph node nodeLabel (edge, targetNode) = do
    J.insert key targetNode (judyGraph jgraph)
    return jgraph
  where
    key = buildWord64 (nodeWithLabel node nodeLabel) edge


-----------------------------------------------------------------------------------------------
-- Generation / Insertion

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
                JGraph nl el -> IO [Node]
targetNodesJ jgraph = do
  imm <- J.freeze (judyGraph jgraph)
  J.elems imm

-- | All nodes (with duplicates => probably useless)
nodesJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
          JGraph nl el -> IO [Node]
nodesJ jgraph = do
  keys <- nodeEdgesJ jgraph
  values <- targetNodesJ jgraph
  return ((map extractFirstWord32 keys) ++ values)


-- | eg for filtering after 'nodesJ'
hasNodeAttr :: NodeAttribute nl => Node -> nl -> Bool
hasNodeAttr node nLabel = (node .&. bitmask bits) == w32
  where (bits, w32) = fastNodeAttr nLabel


-- | Extract attribute of node
--   Using the fact that all nodes have the same number of bits for attributes and a graph
--   needs to have at least one range
extrAttr :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
            JGraph nl el -> Node -> Word32
extrAttr jgraph node = node .&. bitmask bits
  where (bits, w32) = fastNodeAttr (snd (NonEmpty.head (ranges jgraph)))


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
    (bits, _) = fastNodeAttr (snd (NonEmpty.head (ranges jgraph)))


-- | Map a function (Node -> Word32 -> Word32) over all nodes that keeps the node index but
-- changes the node attribute
mapNodeWithKeyJ :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                   (Node -> Word32 -> Word32) -> JGraph nl el -> IO (JGraph nl el)
mapNodeWithKeyJ f jgraph = do
    ns <- nodeEdgesJ jgraph
    nValues <- mapM (\n -> J.lookup n j) ns
    let nodeValues = map (fromMaybe 0) nValues
    deleteNodeEdgeListJ jgraph ns
    let newNode n = newNodeAttr bits (f (extractFirstWord32 n)) n
    let newNs = map newNode ns
    mapM_ (\(key,value) -> J.insert key value j) (zip newNs nodeValues)
    return jgraph
  where
    j = judyGraph jgraph
    (bits, _) = fastNodeAttr (snd (NonEmpty.head (ranges jgraph)))

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


----------------------------------------------------------------------------------------------

-- | Used by union
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
    mapM_ (\(key,value) -> J.insert key value j) nodeEdges
    return ()


----------------------------------------------------------------------------------
-- Query

-- | return a single node
adjacentNodeByAttr :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                       JGraph nl el -> Node -> el -> IO (Maybe Node)
adjacentNodeByAttr jgraph node el =
    J.lookup key j
  where
    nl = nodeLabel jgraph node
    (bits, attr) = fastEdgeAttr el
    key = -- Debug.Trace.trace ("adjacentNodeByAttr "++ show node ++" "++ showHex32 attr
          --                     ++" "++ showHex (buildWord64 node attr)) $
          buildWord64 node attr
    j = judyGraph jgraph


-- | The function that is the purpose of the whole library.
--
-- Because all edges with the same attribute bits can be enumerated, n adjacent nodes with the
-- same attribute can be retrieved with a calculated index and a judy lookup.
-- Eg you have 100.000 edges and 10 edges with a certain attribute, there are only 11 lookups
-- instead of 100.000 with other libraries.
adjacentNodesByAttr :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
                       JGraph nl el -> Node -> el -> IO [(EdgeAttr32, Node)]
adjacentNodesByAttr jgraph node el = do
    n <- J.lookup key j
    maybe (return []) (lookupJudyNodes j node attr 1) n
-- (Debug.Trace.trace ("valAdj "++ show n ++" "++ show node ++" "++ showHex key ++" "++ showHex32 attr) n)
  where
    attr = fastEdgeAttrBase el
    key = buildWord64 node attr
    j = judyGraph jgraph


-- | Recursively increases the index and tries to read a node at
--   edgeAttr + index
lookupJudyNodes :: Judy -> Node -> EdgeAttr32 -> Index -> End -> IO [(EdgeAttr32, Node)]
lookupJudyNodes j node attr i n = do
    val <- J.lookup key j
    next <- if i <= n
-- (Debug.Trace.trace ("lookupJ " ++ showHex32 node ++" "++ showHex32 (attr+i) ++" "++ show val) n)
                 then lookupJudyNodes j node attr (i+1) n
                 else return []
    return (if isJust val then (attr + i, fromJust val) : next
                          else next)
  where
    key = buildWord64 node (attr + i)


-------------------------------------------------------------------------
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
nodeLabel :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
             JGraph nl el -> Node -> nl
nodeLabel jgraph node = nl (ranges jgraph)
  where nl rs | (length rs >= 2) &&
                node >= fst (NonEmpty.head rs) &&
                node < fst (head (NonEmpty.tail rs)) = snd (NonEmpty.head rs)
              -- ((range0,label0):(range1,label1):rs)
              | length rs == 1 = snd (NonEmpty.head rs)
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
        pokeByteOff p 0 w
        peek (castPtr p)


{-# INLINE extractSecondWord32 #-}
extractSecondWord32 :: Word -> Word32
extractSecondWord32 w
    = unsafePerformIO . allocaBytes 4 $ \p -> do
        pokeByteOff p 0 w
        peek (castPtr (plusPtr p 4))

------------------------------------------------
-- Debugging

showHex :: Word -> String
showHex n = showIt 16 n ""
   where
    showIt :: Int -> Word -> String -> String
    showIt 0 _ r = r
    showIt i x r = case quotRem x 16 of
                       (y, z) -> let c = intToDigit (fromIntegral z)
                                 in c `seq` showIt (i-1) y (c:r)

showHex32 :: Word32 -> String
showHex32 n = showIt 8 n ""
   where
    showIt :: Int -> Word32 -> String -> String
    showIt 0 _ r = r
    showIt i x r = case quotRem x 16 of
                       (y, z) -> let c = intToDigit (fromIntegral z)
                                 in c `seq` showIt (i-1) y (c:r)


-- Generate a file that can be displayed for debugging
debugToCSV :: (EdgeAttribute el, Show el) => Edge -> el -> IO ()
debugToCSV (n0,n1) edgeLabel =
  do Text.appendFile "ghc-core-graph/csv/debugNodes.csv"
                     (Text.pack (show n0 ++ "\n" ++ show n1 ++ "\n"))
     Text.appendFile "ghc-core-graph/csv/debugEdges.csv"
                     (Text.pack (show n0 ++ ","  ++ show n1 ++ "," ++ show edgeLabel ++ "\n"))

