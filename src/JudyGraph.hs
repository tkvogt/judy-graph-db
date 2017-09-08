{-# LANGUAGE DeriveGeneric, OverloadedStrings #-}
module JudyGraph where

import           Control.Monad(foldM)
import           Data.Bits((.&.), (.|.))
import qualified Data.Judy as J
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing, maybe, catMaybes, fromMaybe)
import qualified Data.Set as Set
import           Data.Set(Set)
import qualified Data.Text as T
import           Data.Text(Text)
import qualified Data.Vector.Unboxed as VU
import           Data.Word(Word8, Word16, Word32)
import           Foreign.Marshal.Alloc(allocaBytes)
import           Foreign.Ptr(castPtr)
import           Foreign.Storable(peek, pokeByteOff)
import           System.IO.Unsafe(unsafePerformIO)

import Debug.Trace

type Judy = J.JudyL Word32

data NodeLabel = TN TypeNode
               | FN FunctionNode
               | AN AppNode

type TypeNode = Word32
type FunctionNode = Word32
type AppNode = Word32

data EdgeLabel = Word32  -- can be complex
type Node = Word32
type Edge = (Node,Node)
type RangeEnd = Word32
type Bits = Int

-- A graph consists of nodes and edges, that point from one node to another.
-- To encode this with a key-value storage (Judy Arrays), we encode the key as a (node,edge)-pair
-- and the value as a node:

--        key           -->   value

--     node | edge            node
--   ---------------    -->   ------
--   32 bit | 32 bit          32 bit

-- To check properties of edges and nodes fast, we allow to use some of the node/edge bits for these
-- properties. This is called fastAttr and can be up to 24 bits. It depends on your 
-- usage scenario if you plan to use 4 bilion nodes (2^32) or if you need to check properties of a lot
-- of nodes/edges quickly and for example 2^24 nodes are enough, having 8 bits to encode a property.

-- If properties don't fit into n bits (n=1..24), they can be stored in an extra value-node with
-- 64 bits. If this is still not enough, a map is used to associate a node/edge index with any label.

-- Node indexes can be in ranges: If you have lets say 5 different types of nodes, you can store each
-- type of node in a certain range and change the meaning of the edge property bits depending on the
-- range the node index is in. Example:
-- Noderange 0-10: Person node,  8 bits to encode age years in edge between person node and age node
--           11-20: company node, 16 bits to encode the job position in edge between company node and
--                                        job-position node
--          But the number of fastAttr bits in the node has to be constant,
--          because you are either wasting space or overflowing


data JGraph nl el =
  JGraph { graph     :: Judy, -- A Graph with 32 bit keys on the edge
           enumGraph :: Judy, -- enumerate the edges of the first graph, with counter
           complexNodeLabelMap :: Maybe (Map Word32 nl), -- a node attr that does not fit into 64 bit
           complexEdgeLabelMap :: Maybe (Map (Node,Node) [el]),-- an edge attr that doesnt fit into 64bit
           ranges :: [(RangeEnd, nl)]-- an attribute for every range
         }


class NodeAttribute nl where
    fastNodeAttr :: nl -> (Bits, Word32) -- take a label and transforms it into n bits that
                                     -- should be unused in the trailing n bits of the 32bit index
    fastNodeEdgeAttr :: nl -> (el -> (Bits, Word32))


class EdgeAttribute el where
    fastEdgeAttr :: NodeAttribute nl => JGraph nl el -> Node -> el -> (Bits, Word32)
  --   main attr of the arbitraryKeygraph
  --   e.g. unicode leaves 10 bits of the 32 bits unused, that could be used for the direction 
  -- of the edge, if its a right or left edge in a binary tree, etc.



instance NodeAttribute NodeLabel where
    fastNodeAttr (TN nl) = (8, (fromIntegral (nl .&. 0xf000))) -- take 8 leading bit

    fastNodeEdgeAttr (TN nl) el = (8,0) -- (Bits, Word32))
    fastNodeEdgeAttr (FN nl) el = (0,0)
    fastNodeEdgeAttr (AN nl) el = (0,0)


instance EdgeAttribute EdgeLabel where
    fastEdgeAttr jgraph node el = fastNodeEdgeAttr (nodeLabel jgraph node) el


nodeLabel :: NodeAttribute nl => JGraph nl el -> Node -> nl
nodeLabel jgraph node = nl (ranges jgraph)
  where nl ((range,label):rs) | node >= range = label
                              | otherwise     = nl rs


------------------
-- graph interface


------------------------------------------------------------------------------------------------
-- generation / insertion

empty :: Judy -> Judy -> JGraph nl el
empty j mj = JGraph j mj Nothing Nothing []


-- To generate a graph you have to generate two empty Judy arrays (of type Judy = J.JudyL Word32)
-- in your main function (or another IO function) and pass them here
fromList :: (NodeAttribute nl, EdgeAttribute el) =>
            Judy -> Judy -> [(Node, nl)] -> [(Edge, [el])] -> IO (JGraph nl el)
fromList j mj nodes edges = do
    let jgraph = empty j mj
    ngraph <- foldM insertNode jgraph nodes
    egraph <- foldM insertEdge ngraph edges
    return egraph


-- No usage of secondary Data.Map structure
-- Faster and more memory efficient but labels have to fit into less than 32 bits
fromListJudy :: (NodeAttribute nl, EdgeAttribute el) =>
                Judy -> Judy -> [(Edge, Maybe nl, [el])] -> IO (JGraph nl el)
fromListJudy j mj nodeEdges = do
    let jgraph = empty j mj
    mapM (insertNodeEdge jgraph) nodeEdges
    return jgraph


---------------------------------------------------------------------------------------------------

-- Inserting a new node means either
--  * if its new then only add an entry to the secondary map
--  * if it already exists then the label of the existing node is changed.
--    This can be slow because all node-edges are updated (O(#adjacentEdges))
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


updateNodeEdges :: NodeAttribute nl =>
                   JGraph nl el -> Node -> nl -> (Word32, Word32) -> IO (JGraph nl el)
updateNodeEdges jgraph node nodeLabel (edge, targetNode) = do
    J.insert key targetNode (graph jgraph)
    return jgraph
  where
    key = buildWord64 (nodeWithLabel node nodeLabel) edge


insertEdge :: (NodeAttribute nl, EdgeAttribute el) =>
              JGraph nl el -> (Edge, [el]) -> IO (JGraph nl el)
insertEdge jgraph ((n0,n1), edgeLabels) = do
    insertNodeEdge jgraph ((n0, n1), nLabel, edgeLabels)
    return (jgraph { complexEdgeLabelMap = Just newEdgeLabelMap })
  where
    nLabel = maybe Nothing (Map.lookup n0) (complexNodeLabelMap jgraph)

    -- Using the secondary map for more detailed data
    oldLabel = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap jgraph)
    newEdgeLabelMap = Map.insert (n0,n1)
                          ((fromMaybe [] oldLabel) ++ edgeLabels) -- multi edges between the same nodes
                          (fromMaybe Map.empty (complexEdgeLabelMap jgraph))


-- build the graph without using the secondary Data.Map graph
insertNodeEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                  JGraph nl el -> (Edge, Maybe nl, [el]) -> IO (JGraph nl el)
insertNodeEdge jgraph ((n0, n1), nodeLabel, edgeLabels) = do
    -- the enumgraph has the number of adjacent edges at index 0
    let enumNodeEdge = buildWord64 n0Key 0
    edgecount <- J.lookup enumNodeEdge mj
    if isNothing edgecount then J.insert enumNodeEdge 1 mj -- the first edge is added, set counter to 1
                           else J.insert enumNodeEdge ((fromJust edgecount)+1) mj -- incse counter by 1
    let enumKey = buildWord64 n0Key ((fromMaybe 0 edgecount)+1)
    J.insert key n1 j
    J.insert enumKey n1 mj
    return jgraph
  where
    j = graph jgraph
    mj = enumGraph jgraph
    n0Key = if isJust nodeLabel then nodeWithLabel n0 (fromJust nodeLabel) else n0
    key = 0
--    keys = map (buildWord64 n0Key) edgeLabels


insertEdgeSet :: (NodeAttribute nl, EdgeAttribute el) =>
                 JGraph nl el -> Map Edge [el] -> IO (JGraph nl el)
insertEdgeSet jgraph set = do
    let edges = Map.toList set
    newGraph <- foldM insertEdge jgraph edges
    return newGraph


--------------------------------------------------------------------------------------
-- TODO
union :: JGraph nl el -> JGraph nl el -> JGraph nl el
union (JGraph graph0 enumGraph0 complexNodeLabel0 complexEdgeLabel0 ranges0)
      (JGraph graph1 enumGraph1 complexNodeLabel1 complexEdgeLabel1 ranges1) =
      (JGraph graph0 enumGraph0 Nothing Nothing [])

---------------------------------------------------------------------------------------
-- deletion

-- TODO: enumGraph has to be adjusted too
deleteNode :: NodeAttribute nl => JGraph nl el -> Node -> IO (JGraph nl el)
deleteNode jgraph node = do
    let newNodeMap = maybe Nothing (Just . (Map.delete node)) (complexNodeLabelMap jgraph)
--    J.delete key mj
    return (jgraph{ complexNodeLabelMap = newNodeMap })
  where nl = nodeWithMaybeLabel node (maybe Nothing (Map.lookup node) lmap)
        mj = graph jgraph
        lmap = complexNodeLabelMap jgraph


-- deleteJudyEdges :: JGraph nl el -> [Word]


deleteNodeSet :: NodeAttribute nl => JGraph nl el -> Set Node -> IO (JGraph nl el)
deleteNodeSet jgraph set = do
    newNodeMap <- foldM deleteNode jgraph (Set.toList set)
    return (jgraph{ complexNodeLabelMap = complexNodeLabelMap newNodeMap })

-- TODO: changes in judy array
deleteEdge :: JGraph nl el -> Edge -> IO (JGraph nl el)
deleteEdge jgraph (n0,n1) = do
    let newEdgeMap = maybe Nothing (Just . (Map.delete (n0,n1))) (complexEdgeLabelMap jgraph)
    return (jgraph { complexEdgeLabelMap = newEdgeMap })


----------------------------------------------------------------------------------------
-- helper functions


{-# INLINE buildWord64 #-}
buildWord64 :: Word32 -> Word32 -> Word
buildWord64 w0 w1
    = unsafePerformIO . allocaBytes 8 $ \p -> do
        pokeByteOff p 0 w0
        pokeByteOff p 4 w1
        peek (castPtr p)


nodeWithLabel :: NodeAttribute nl => Node -> nl -> Word32
nodeWithLabel node nl = node .|. (snd (fastNodeAttr nl))

nodeWithMaybeLabel :: NodeAttribute nl => Node -> Maybe nl -> Word32
nodeWithMaybeLabel node Nothing = node
nodeWithMaybeLabel node (Just nl) = node .|. (snd (fastNodeAttr nl))

----------------------------------------------------------------------------------------------------
-- query

isEmpty :: JGraph nl el -> IO Bool
isEmpty (JGraph graph enumGraph _ _ _) = J.null graph


-- TODO: A direct pointer to the complex label (but how?)
lookupNode :: JGraph nl el -> Word32 -> Maybe nl
lookupNode graph n = maybe Nothing (Map.lookup n) (complexNodeLabelMap graph)


-- TODO: It would be a little bit quicker to have a tree searching for n1
--        because in most cases n0 is already looked up
lookupEdge :: JGraph nl el -> Edge -> Maybe [el]
lookupEdge graph (n0,n1) = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap graph)


-- Fast property bits can be extracted from a complex edge attribute with a typeclass instance.
-- They are then stored in the 64 bit node-edge-key.
-- Because all edges with the same property bits can be enumerated, all adjacent nodes with the same
-- property can be retrieved with a calculated index and a judy lookup until the last judy lookup fails.
-- Eg you have 100.000 edges and 10 edges with a certain property, there are only 11 lookups
-- instead of 100.000 with other libraries.
adjacentNodesByProp :: (NodeAttribute nl, EdgeAttribute el) =>
                       JGraph nl el -> Node -> EdgeLabel -> IO (Set Word32)
adjacentNodesByProp jgraph node el = do
    ns <- lookupJudyNode jgraph node el 0
    return (Set.fromList ns)


lookupJudyNode :: (NodeAttribute nl, EdgeAttribute el) =>
                  JGraph nl el -> Node -> EdgeLabel -> Word32 -> IO [Word32]
lookupJudyNode jgraph node el i = do
    n <- J.lookup key j
    next <- if isJust n then lookupJudyNode jgraph node el (i+1) else return []
    return ((fromJust n) : next)

  where nl = nodeLabel jgraph node
        (bits, attr) = fastNodeEdgeAttr nl el
        key :: Word
        key = buildWord64 node (attr + i)
        j = graph jgraph
        mj = enumGraph jgraph


adjacentNodeCount :: (NodeAttribute nl, EdgeAttribute el) =>
                     JGraph nl el -> Word32 -> IO Word32
adjacentNodeCount jgraph node = do
    let enumNodeEdge = buildWord64 (nodeWithMaybeLabel node nl) 0 --the first index lookup is the count
    numEdges <- J.lookup enumNodeEdge (enumGraph jgraph)
    if isJust numEdges then return (fromJust numEdges) else return 0
  where nl = maybe Nothing (Map.lookup node) (complexNodeLabelMap jgraph)
        mj = graph jgraph


-- Useful if you want all adjacent edges, but you cannout check all 32 bit words with a lookup
-- and the distribution of the fast attributes follows no rule, there is no other choice but to 
-- enumerate all edges. Eg you reserve 24 bit for a unicode char, but only 50 chars are used.
-- But some algorithms need all adjacent edges, for example merging results of leaf nodes in a tree
-- recursively until the root is reached
adjacentNodesByIndex :: JGraph nl el -> Word32 -> (Int,Int) -> IO (Set Word32)
adjacentNodesByIndex jgraph n (start, end) = do
--    (enumGraph jgraph)
    return Set.empty


-- The enumGraph enumerates all child edges
-- and maps to the second 32 bit of the key of all nodeEdges
allChildEdges :: JGraph nl el -> Word32 -> IO [Word32]
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


allChildNodes :: JGraph nl el -> Word32 -> IO [Word32]
allChildNodes jgraph node = do
    edges <- allChildEdges jgraph node
    allChildNodesFromEdges jgraph edges node


-- to avoid the recalculation of edges
allChildNodesFromEdges :: JGraph nl el -> [Word32] -> Word32 -> IO [Word32]
allChildNodesFromEdges jgraph edges node = do
    let keys = map (buildWord64 node) edges
    nodes <- mapM (\key -> J.lookup key j) keys
    return (-- Debug.Trace.trace ("ec " ++ show enumKeys) $
            catMaybes nodes)
  where
    j = graph jgraph


-- TODO: more functions

-- mapNode :: (nl0 -> nl1) -> Graph e n el nl0 -> Graph e n el nl1

-- mapNodeWithKey :: (n -> nl0 -> nl1) -> Graph e n el nl0 -> Graph e n el nl1

----------------------------------------------------------------------------------------
-- Cypher Query Interface

data CypherEdge = P String -- property string
                | V String -- variable length path: "*", "*1..5", "*..6"

data NodeType = Function | Type | App | Lit

data EncodedEdge = R | L | OutType | InType1_30 | NextArg


data CypherNode = Any

data CypherResult a = CyGraph Judy
                    | Rows [a] -- rows



executeOn :: JGraph nl el -> CypherNode -> IO (CypherResult a)
executeOn judyGraph cypher = do
    return (Rows [])


anyNode :: CypherNode
anyNode = Any


(--|) :: CypherNode -> CypherEdge -> CypherEdge
(--|) node edge = edge

(|--) :: CypherEdge -> CypherNode -> CypherNode
(|--) edge node = node


(<--|) :: CypherNode -> CypherEdge -> CypherEdge
(<--|) node edge = edge

(|-->) :: CypherEdge -> CypherNode -> CypherNode
(|-->) edge node = node


(-~-) :: CypherNode -> CypherNode -> CypherNode
(-~-) node0 node1 = node0

(-->) :: CypherNode -> CypherNode -> CypherNode
(-->) node0 node1 = node0

(<--) :: CypherNode -> CypherNode -> CypherNode
(<--) node0 node1 = node0

infixl 7 --|
infixl 7 |--
infixl 7 <--|
infixl 7 |-->
infixl 7 -~-
infixl 7 -->
infixl 7 <--


----------------------------------------------------------
-- Cypher examples

exampleCypher = anyNode --| P "LIKES" |--> anyNode

--  executeOn

