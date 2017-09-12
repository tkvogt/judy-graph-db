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

-- To check attributes of edges and nodes fast, we allow to use some of the node/edge bits for these
-- attributes. This is called fastAttr and can be up to 32 bits. The number of fastAttr bits in the
-- node has to be constant, because you are either wasting space or overflowing.
-- It depends on your usage scenario if you plan to use 4 bilion nodes (2^32) or if you need to check 
-- attributes of a lot of nodes/edges quickly and for example 2^24 nodes are enough, having 8 bits
-- to encode an attribute:

--     node | node attr |  edge  | edge attr           node
--   ----------------------------------------   -->   ------
--   24 bit |  8 bit    | 16 bit | 16 bit             32 bit


-- If attributes don't fit into n bits (n=1..24), they can be stored in an extra value-node with
-- 64 bits. If this is still not enough,
-- a secondary data.map is used to associate a node/edge with any label.

-- Edge Attributes depending on ranges
--------------------------------------
-- Node indexes can be in ranges: If you have lets say 5 different types of nodes, you can store each
-- type of node in a certain range and change the meaning of the edge attribute bits depending on the
-- range the node index is in. Example:
-- Noderange 0-10: Person node,  8 bits to encode age years in an edge between each person node
--                 and company node.
--           11-20: company node, 16 bits to encode the job position in an edge between a company node
--                  an a person node.
-- The rest of the bits are used to enumerate the the edges with this attribute.
-- Eg you have encoded the job position in the edge then the rest of the bits can be used to
-- enumerate the people with this job position. If there are 10 edges out of 100.000 with a
-- certain attribute, then we can access these edges in 10n steps.
--
-- Too many ranges obviously slow down the library.

data JGraph nl el =
  JGraph { graph     :: Judy, -- A Graph with 32 bit keys on the edge
           enumGraph :: Judy, -- enumerate the edges of the first graph, with counter
           complexNodeLabelMap :: Maybe (Map Word32 nl),       --a  node attr that doesnt fit into 64bit
           complexEdgeLabelMap :: Maybe (Map (Node,Node) [el]),--an edge attr that doesnt fit into 64bit
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
    mapM (insertNodeEdges jgraph) nodeEdges
    return jgraph


---------------------------------------------------------------------------------------------------

-- Inserting a new node means either
--  * if its new then only add an entry to the secondary data.map
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
    insertNodeEdges jgraph ((n0, n1), nLabel, edgeLabels)
    return (jgraph { complexEdgeLabelMap = Just newEdgeLabelMap })
  where
    nLabel = maybe Nothing (Map.lookup n0) (complexNodeLabelMap jgraph)

    -- Using the secondary map for more detailed data
    oldLabel = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap jgraph)
    newEdgeLabelMap = Map.insert (n0,n1)
                          ((fromMaybe [] oldLabel) ++ edgeLabels) -- multi edges between the same nodes
                          (fromMaybe Map.empty (complexEdgeLabelMap jgraph))


insertEdgeSet :: (NodeAttribute nl, EdgeAttribute el) =>
                 JGraph nl el -> Map Edge [el] -> IO (JGraph nl el)
insertEdgeSet jgraph set = do
    let edges = Map.toList set
    newGraph <- foldM insertEdge jgraph edges
    return newGraph


insertNodeEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                   JGraph nl el -> (Edge, Maybe nl, [el]) -> IO ()
insertNodeEdges jgraph ((n0, n1), nodeLabel, edgeLabels) = do
    mapM (insertNodeEdge jgraph) (map (\el -> ((n0, n1), nodeLabel, el)) edgeLabels)
    return ()


-- Build the graph without using the secondary Data.Map graph
insertNodeEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                  JGraph nl el -> (Edge, Maybe nl, el) -> IO ()
insertNodeEdge jgraph ((n0, n1), nl, edgeLabel) = do
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
    J.insert newValKey n1 j

  where
    j = graph jgraph
    mj = enumGraph jgraph
    n0Key = if isJust nl then nodeWithLabel n0 (fromJust nl) else n0
    key i = buildWord64 n0Key (i + (snd $ fastNodeEdgeAttr nLabel edgeLabel))
    nLabel = if isJust nl then fromJust nl else nodeLabel jgraph n0


--------------------------------------------------------------------------------------

union :: JGraph nl el -> JGraph nl el -> IO (JGraph nl el)
union (JGraph graph0 enumGraph0 complexNodeLabelMap0 complexEdgeLabelMap0 ranges0)
      (JGraph graph1 enumGraph1 complexNodeLabelMap1 complexEdgeLabelMap1 ranges1) = do

    ((biggerJudy,biggerJudyEnum), (smallerJudy, smallerJudyEnum)) <- biggerSmaller (graph0,enumGraph0) (graph1,enumGraph1)
    nodeEdges <- getNodeEdges (smallerJudy, smallerJudyEnum)
    (newJudyGraph, newJudyEnum) <- insertNE nodeEdges (biggerJudy, biggerJudyEnum)

    return (JGraph newJudyGraph newJudyEnum
            (mapUnion complexNodeLabelMap0 complexNodeLabelMap1)
            (mapUnion complexEdgeLabelMap0 complexEdgeLabelMap1)
            ranges0) -- assuming ranges are global
  where

    mapUnion (Just complexLabelMap0) (Just complexLabelMap1) = Just (Map.union complexLabelMap0 complexLabelMap1)
    mapUnion Nothing (Just complexLabelMap1) = Just complexLabelMap1
    mapUnion (Just complexLabelMap0) Nothing = Just complexLabelMap0
    mapUnion Nothing Nothing = Nothing

    biggerSmaller :: (Judy,Judy) -> (Judy,Judy) -> IO ((Judy,Judy), (Judy,Judy))
    biggerSmaller (g0,e0) (g1,e1) = do
       s0 <- J.size g0
       s1 <- J.size g1
       if s0 >= s1 then return ((g0,e0),(g1,e1)) else return ((g1,e1),(g0,e0))

    insertNE :: [(Word, Word32)] -> (Judy, Judy) -> IO (Judy, Judy)
    insertNE nodeEdges (j,mj) = return (j,mj)

    getNodeEdges :: (Judy,Judy) -> IO [(Word, Word32)]
    getNodeEdges (j,mj) = return []


---------------------------------------------------------------------------------------
-- Deletion

-- TODO: enumGraph has to be adjusted too
deleteNode :: NodeAttribute nl => JGraph nl el -> Node -> IO (JGraph nl el)
deleteNode jgraph node = do
    let newNodeMap = maybe Nothing (Just . (Map.delete node)) (complexNodeLabelMap jgraph)
--    J.delete key mj
    return (jgraph{ complexNodeLabelMap = newNodeMap })
  where
    nl = nodeWithMaybeLabel node (maybe Nothing (Map.lookup node) lmap)
    mj = graph jgraph
    lmap = complexNodeLabelMap jgraph


-- deleteJudyEdges :: JGraph nl el -> [Word]


deleteNodeSet :: NodeAttribute nl => JGraph nl el -> Set Node -> IO (JGraph nl el)
deleteNodeSet jgraph set = do
    newNodeMap <- foldM deleteNode jgraph (Set.toList set)
    return (jgraph{ complexNodeLabelMap = complexNodeLabelMap newNodeMap })


-- TODO: changes in judy array
-- This will produce holes in the continuously enumerated edge list
-- that maybe have to be garbage collected with deleteAllEdgesWithAttr
deleteEdge :: JGraph nl el -> Edge -> IO (JGraph nl el)
deleteEdge jgraph (n0,n1) = do
    let newEdgeMap = maybe Nothing (Just . (Map.delete (n0,n1))) (complexEdgeLabelMap jgraph)
    return (jgraph { complexEdgeLabelMap = newEdgeMap })

-- TODO
-- A primitive alternative to garbage collection, because deleteEdge will produce holes
-- delete as many edges as the counter says there are and then set counter to 0
deleteAllEdgesWithAttr :: (NodeAttribute nl, EdgeAttribute el) =>
                          JGraph nl el -> el -> IO (JGraph nl el)
deleteAllEdgesWithAttr jgraph el = return jgraph


----------------------------------------------------------------------------------------
-- Helper functions


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
-- Query

isEmpty :: JGraph nl el -> IO Bool
isEmpty (JGraph graph enumGraph _ _ _) = J.null graph


-- TODO: A direct pointer to the complex label (but how?)
lookupNode :: JGraph nl el -> Word32 -> Maybe nl
lookupNode graph n = maybe Nothing (Map.lookup n) (complexNodeLabelMap graph)


-- TODO: It would be a little bit quicker to have a tree searching for n1
--        because in most cases n0 is already looked up
lookupEdge :: JGraph nl el -> Edge -> Maybe [el]
lookupEdge graph (n0,n1) = maybe Nothing (Map.lookup (n0,n1)) (complexEdgeLabelMap graph)


-- Fast attribute bits can be extracted from a complex edge attribute with a typeclass instance.
-- They are then stored in the 64 bit node-edge-key.
-- Because all edges with the same attribute bits can be enumerated, all adjacent nodes with the same
-- attribute can be retrieved with a calculated index and a judy lookup until the last judy lookup fails.
-- Eg you have 100.000 edges and 10 edges with a certain attribute, there are only 11 lookups
-- instead of 100.000 with other libraries.
adjacentNodesByAttr :: (NodeAttribute nl, EdgeAttribute el) =>
                       JGraph nl el -> Node -> EdgeLabel -> IO (Set Word32)
adjacentNodesByAttr jgraph node el = do
    n <- J.lookup key j
    if isJust n then fmap Set.fromList (lookupJudyNode j node attr 1 (fromJust n))
                else return Set.empty
  where
    nl = nodeLabel jgraph node
    (bits, attr) = fastNodeEdgeAttr nl el
    key = buildWord64 node attr
    j = graph jgraph


lookupJudyNode :: Judy -> Node -> Word32 -> Word32 -> Word32 -> IO [Word32]
lookupJudyNode j node el i n = do
    val <- J.lookup key j
    next <- if i <= n then lookupJudyNode j node el (i+1) n else return []
    return (if isJust val then ((fromJust val) : next) else next)
  where
    key = buildWord64 node (el + i)


-- Useful if you want all adjacent edges, but you cannout check all 32 bit words with a lookup
-- and the distribution of the fast attributes follows no rule, there is no other choice but to 
-- enumerate all edges. Eg you reserve 24 bit for a unicode char, but only 50 chars are used.
-- But some algorithms need all adjacent edges, for example merging results of leaf nodes in a tree
-- recursively until the root is reached
adjacentNodesByIndex :: JGraph nl el -> Word32 -> (Word32, Word32) -> IO (Set Word32)
adjacentNodesByIndex jgraph node (start, end) = do
    val <- J.lookup key mj
    if isJust val then fmap Set.fromList (lookupJudyNode mj node 0 start end)
                  else return Set.empty
  where
    key = buildWord64 node 0
    mj = enumGraph jgraph

---------------------------------------------------------------

adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> Word32 -> IO Word32
adjacentEdgeCount jgraph node = do
    let edgeCountKey = buildWord64 (nodeWithMaybeLabel node nl) 0 --the first index lookup is the count
    edgeCount <- J.lookup edgeCountKey mj
    if isJust edgeCount then return (fromJust edgeCount) else return 0
  where
    nl = maybe Nothing (Map.lookup node) (complexNodeLabelMap jgraph)
    mj = enumGraph jgraph


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


---------------------------------------------------------------------------------------
-- Changing all nodes

-- If the graph was constructed with insertNode or fromList these maps will be used to
-- access the node-edges. Otherwise a breadth-first-search is done with enumgraph, assuming a DAG.
-- In this case you have to keep in mind that an inserted edge points from n0 to n1,
-- but n1 does not know it was pointed at.

mapNode :: (nl0 -> nl1) -> JGraph nl el -> IO (JGraph nl el)
mapNode f jgraph = return jgraph


mapNodeWithKey :: (n -> nl0 -> nl1) -> JGraph nl el -> IO (JGraph nl el)
mapNodeWithKey f jgraph = return jgraph


----------------------------------------------------------------------------------------
-- Cypher Query Interface

data CypherEdge = P String -- attribute string
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

