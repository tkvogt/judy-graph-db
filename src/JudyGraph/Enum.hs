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
    Edge, Node32(..), Edge32(..), NodeEdge, RangeStart, RangeLen, Index, Start, End, Bits,
    -- * Construction
    empty, fromList, emptyE, fromListE,
    insertNodeEdge2, insertNodeLines, insertNE,
    updateNodeEdges, mapNodeJ, mapNodeWithKeyJ,
    -- * Extraction
    getNodeEdges, nodeEdgesJ, nodesJ,
    -- * Deletion
    deleteNodeEdgeListJ, deleteNodeEdgeListE,
    -- * Query
    adjacentNodesByAttr, adjacentNodeByAttr, lookupJudyNodes, adjacentEdgeCount,
    allChildEdges, allChildNodes, allChildNodesFromEdges, allAttrBases,
    adjacentEdgesByIndex, adjacentNodesByIndex,
    -- * Handling Labels
    nodeWithLabel, nodeWithMaybeLabel, nodeLabel,
    hasNodeAttr, extrAttr, newNodeAttr, bitmask, invBitmask,
    buildWord64, extractFirstWord32, extractSecondWord32, edgeBackward,
    -- * Displaying in hex for debugging
    showHex, showHex32
  ) where

import           Control.Monad
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BL
import qualified Data.Char8 as C
import qualified Data.Csv as CSV
import qualified Data.Judy as J
import           Data.List.NonEmpty(NonEmpty(..), toList)
import           Data.Maybe(fromJust, isJust, isNothing, maybe, catMaybes, fromMaybe)
import qualified Data.Vector as V
import           Data.Vector(Vector)
import           Data.Text(Text)
import qualified Data.Text as T
import           Data.Text.Encoding
import           Data.Word(Word32)
import qualified Streamly as S
import qualified Streamly.Prelude as S
import           System.IO.Unsafe(unsafePerformIO)
import           System.IO (IOMode (ReadMode), withFile)
import qualified JudyGraph.FastAccess as JF
import           JudyGraph.FastAccess
-- import Debug.Trace

-- | The edges are enumerated, because sometimes the edge attrs are not continuous
--   and it is impossible to try all possible 32 bit attrs
data (NodeAttribute nl, EdgeAttribute el) =>
     EnumGraph nl el = EnumGraph {
  judyGraphE :: Judy, -- ^ A Graph with 32 bit keys on the edge
  enumGraph :: Judy,-- ^ Enumerate the edges of the first graph,with counter at position 0.
                    --   Deletions in the first graph are not updated here (too costly)
  rangesE :: NonEmpty ((RangeStart, RangeLen), (nl, [el])),-- ^ A nonempty list with an attr for every range
                                     -- and assigning which edgelabels are valid in each range
  nodeCountE :: Word32,
  showEdge :: Edge32 -> el
}



instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
         Show (EnumGraph nl el) where
  show (EnumGraph judyGrE _ _ _ showE) =
         "\ndigraph graphviz {\n"++
         concat (zipWith3 line nodeOrigins edges nodeDests) ++
         "}\n"
    where
      nodeOrigins = map extractFirstWord32 $
                    unsafePerformIO (J.keys  (unsafePerformIO (J.freeze judyGrE)))
      edges = map extractSecondWord32 $
              unsafePerformIO (J.keys  (unsafePerformIO (J.freeze judyGrE)))
      nodeDests = unsafePerformIO (J.elems (unsafePerformIO (J.freeze judyGrE)))
      line origin e dest = show origin ++" -> "++ show dest ++" [ label = \""++ 
                       (backLabel e) ++ show (showE (Edge32 e)) ++ "\" ];\n"

-- | Generate two empty judy arrays and two empty data.maps for complex node and edge
--   labels. The purpose of the range list is to give a special interpretation of edges
--   depending on the node type.
empty :: (NodeAttribute nl, EdgeAttribute el) => NonEmpty ((RangeStart, RangeLen), (nl, [el])) -> IO (EnumGraph nl el)
empty rs = do
  j <- J.new :: IO Judy
  mj <- J.new :: IO Judy
  return (EnumGraph j mj rs 0 edgeFromAttr)


fromList :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
            Bool -> [(Node32, nl)]
                 -> [((Node32, Node32), Maybe nl, Maybe nl, [el], Bool)]
                 -> [((Node32, Node32), Maybe nl, Maybe nl, [el])]
                 -> NonEmpty ((RangeStart, RangeLen), (nl, [el]))
                 -> IO (EnumGraph nl el)
fromList overwrite nodes directedEdges nodeEdges rs = do
  jgraph <- empty rs
  insertNodeEdges overwrite jgraph nodes
                  (directedEdges ++ (map addDir nodeEdges) ++ (map dirRev nodeEdges) )
  where addDir ((from,to), nl0, nl1, labels) = ((from,to), nl1, nl0, labels, True)
        dirRev ((from,to), nl0, nl1, labels) = ((to,from), nl1, nl0, labels, True)

------------------------------------------------------------------------------------------

instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
         GraphClass EnumGraph nl el where

  -- | Are the judy arrays of the graph both empty?
  isNull (EnumGraph graph enumGr _ _ _) = do
    g <- J.null graph
    e <- J.null enumGr
    return (g && e)

  addNodeCount nodes gr = gr { nodeCountE = (nodeCountE gr) + fromIntegral (length nodes) }

  insertNodeEdges overwrite jgraph nodes es = fmap (addNodeCount nodes) (foldM foldEs jgraph es)
    where
      foldEs g ((n0, n1), nl0, nl1, edgeLs, dir) = fmap fst $ insertNodeEdgeAttr overwrite g e
        where e = ((n0, n1), nl0, nl1, overlay edgeLs, overlayBase edgeLs)
              overlay el     = Edge32 (sum (map (addDir . snd . fastEdgeAttr) el))
              overlayBase el = Edge32 (sum (map (addDir . fastEdgeAttrBase) el))
              addDir attr | dir = attr
                          | otherwise = attr + edgeBackward

  -- | Build the graph without using the secondary Data.Map graph
  --   If edge already exists and (overwrite == True) overwrite it
  --   otherwise create a new edge and increase counter (that is at index 0)
  insertNodeEdge overwrite jgraph ((Node32 n0, Node32 n1), nl0, nl1, el, dir) =
  --  Debug.Trace.trace ("ins attr"++ show (el, Edge32 (snd (fastEdgeAttr el)))) $
    fmap fst $ insertNodeEdgeAttr overwrite jgraph
                           ((Node32 n0, Node32 n1), nl0, nl1, Edge32 attr, Edge32 attrBase)
   where
    attr =   snd (fastEdgeAttr el) + (if dir then 0 else edgeBackward)
    attrBase = fastEdgeAttrBase el + (if dir then 0 else edgeBackward)

{-
  insertNodeEdgeAttr over gr ((Node32 n0, n1), nl0, nl1, Edge32 attr, attrBase) = do
    (g,(new,(n,edgeAttrCount))) <-
      JF.insertNodeEdgeAttr over gr ((Node32 n0, n1), nl0, nl1, Edge32 attr, attrBase)
    insertEnumEdge g (Node32 n0Key) (Edge32 (attr + if over then 0 else edgeAttrCount))
    return (g,(new,(n,edgeAttrCount)))
   where
    n0Key = maybe n0 (nodeWithLabel (Node32 n0)) nl0
-}

  insertNodeEdgeAttr overwrite jgraph
                     ((Node32 n0, Node32 n1), nl0, nl1, Edge32 attr, Edge32 attrBase) = do
    maybeEdgeAttrCount <- J.lookup edgeAttrCountKey j
    let edgeAttrCount = fromMaybe 0 maybeEdgeAttrCount

    insertEnumEdges jgraph (Node32 n0Key) (Node32 n1Key) (Edge32 attrBase)
                           (Edge32 (attr + if overwrite then 0 else edgeAttrCount))
--    debugToCSV (n0Key,n1Key) edgeLabel
    -------------------------------------
    let newValKey = buildWord64 n0Key (attr + if overwrite then 0 else edgeAttrCount)
    n2 <- J.lookup newValKey j
    let isEdgeNew = isNothing n2
    when (isEdgeNew || (not overwrite)) (J.insert edgeAttrCountKey (edgeAttrCount+1) j)
    J.insert newValKey n1Key j -- (Debug.Trace.trace ("Enum count: "++ showHex edgeAttrCountKey ++" "++ show (n0Key, isEdgeNew, overwrite, (n0,n1), edgeAttrCount) ++" "++ showHex newValKey) j)
    let newN = fromMaybe (Node32 n1) (fmap Node32 n2)
    if isEdgeNew || (not overwrite)
      then return (jgraph { nodeCountE = (nodeCountE jgraph) + 1},
                           (isEdgeNew, (Node32 n1, edgeAttrCount)))
--      then return (jgraph, (isEdgeNew, (Node32 n1, edgeAttrCount)))
      else return (jgraph, (isEdgeNew, (newN, edgeAttrCount)))
   where
    j = judyGraphE jgraph
    n0Key = maybe n0 (nodeWithLabel (Node32 n0)) nl0
    n1Key = maybe n1 (nodeWithLabel (Node32 n1)) nl1
    -- An edge consists of an attribute and a counter
    edgeAttrCountKey = buildWord64 n0Key attrBase

---------------------------------------------------------
  -- | In a dense graph the edges might be too big to be first stored in a list before
  --   being added to the judy graph. Therefore the edges are streamed from a 
  --   .csv-file line by line and then added to the judy-graph. A function is passed that
  --   can take a line (a list
  --   of strings) and add it to the graph.
  insertCSVEdgeStream :: (NodeAttribute nl, EdgeAttribute el, Show el) =>
                          EnumGraph nl el -> FilePath ->
                         (EnumGraph nl el -> Either String (Vector Text) -> IO (EnumGraph nl el))
                       -> IO (EnumGraph nl el)
  insertCSVEdgeStream graph file newEdge = withFile file ReadMode $ \handle -> do
    S.foldlM' newEdge graph
    . S.serially
    . fmap readLine
    . S.fromHandle
    $ handle
    where
      readLine :: String -> Either String (Vector Text)
      readLine line = fmap ((V.map (decodeUtf8 . BL.toStrict)) . V.head) strs
        where strs = CSV.decode CSV.NoHeader l :: Either String (Vector (Vector BL.ByteString))
              l = BL.fromStrict (encodeUtf8 (T.pack line))

  insertCSVEdge newEdge g (Right edgeProp) = newEdge g edgeProp
  insertCSVEdge _       g (Left _)         = return g

----------------------------------------------------------------------------------------
  -- | deletes all node-edges that contain this node, because the judy array only stores
  --   node-edges
  deleteNode jgraph (Node32 node) = do
  --    es <- allChildEdges jgraph node
    let nodeEdges = map (buildWord64 node) [] -- es
    deleteNodeEdgeListE jgraph nodeEdges


  deleteNodes jgraph nodes = do
    _ <- foldM deleteNode jgraph nodes
    return jgraph

  -- | "deleteEdge jgraph (n0, n1)" deletes the edge that points from n0 to n1
  --
  --   It is slow because it uses 'filterEdgesTo'. If possible use 'deleteNodeEdgeList'.
  deleteEdge :: (NodeAttribute nl, EdgeAttribute el) =>
                (EnumGraph nl el) -> Edge -> IO (EnumGraph nl el)
  deleteEdge jgraph (Node32 n0, Node32 n1) = do
    -- es <- allChildEdges jgraph n0
    let nodeEdges = map (buildWord64 n0) [] -- es
    edgesToN1 <- filterEdgesTo jgraph nodeEdges (\(Edge32 node) -> node == n1)
    deleteNodeEdgeListE jgraph edgesToN1


  deleteEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                (EnumGraph nl el) -> [Edge] -> IO (EnumGraph nl el)
  deleteEdges jgraph edges = do
    _ <- foldM deleteEdge jgraph edges
    return jgraph

----------------------------------------------------------------------------------------

  union gr0 gr1 = do
    ((EnumGraph bg be br bn se0), (EnumGraph sg se _ _ _)) <- biggerSmaller gr0 gr1
    nodeEs   <- getNodeEdges sg
    nodeEsMj <- getNodeEdges se
    insertNE nodeEs   bg
    insertNE nodeEsMj be
    return (EnumGraph bg be br bn se0)
   where
    biggerSmaller :: (NodeAttribute nl, EdgeAttribute el) =>
             EnumGraph nl el -> EnumGraph nl el -> IO (EnumGraph nl el, EnumGraph nl el)
    biggerSmaller (EnumGraph g0 e0 r0 n0 se0) (EnumGraph g1 e1 r1 n1 se1) = do
       s0 <- J.size g0
       s1 <- J.size g1
       if s0 >= s1 then return ((EnumGraph g0 e0 r0 n0 se0), (EnumGraph g1 e1 r1 n1 se1))
                   else return ((EnumGraph g1 e1 r1 n1 se1), (EnumGraph g0 e0 r0 n0 se0))

----------------------------------------------------------------------------------------

  -- | Introduced for the cypher interface
  --   Makes a lookup to see how many edges there are
  -- TODO: Should they also lookup the target nodes?
  --       Currently Yes, just to make sure they exist
  adjacentEdgesByAttr jgraph (Node32 node) (Edge32 attr) = do
    n <- J.lookup key j
    map fst <$> lu n
   where
    key = buildWord64 node attr
    j = judyGraphE jgraph
    lu :: Maybe Word32 -> IO [(Edge32, Node32)]
    lu n | isJust n =
   --        Debug.Trace.trace ("eAdj "++ show (n,node) ++ showHex32 attr ++" "++ showHex key)
           lookupJudyNodes j (Node32 node) (Edge32 attr) True (Node32 1) (Node32 (fromJust n))
         | otherwise =
   --        Debug.Trace.trace ("eAdj2 "++ show (n,node) ++ showHex32 attr ++" "++ showHex key)
           return []

  -- | The Judy array maps a NodeEdge to a target node
  --
  --   Keep those NodeEdges where target node has a property (Word32 -> Bool)
--  filterEdgesTo :: graph nl el -> [NodeEdge] -> (Edge32 -> Bool) -> IO [NodeEdge]
  filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> fmap (fmap Edge32) (J.lookup n j)) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
   where j = judyGraphE jgraph
         filterNode (_, Just v) | f v = True
                                | otherwise = False
         filterNode _ = False

  nodeCount graph = nodeCountE graph
  ranges :: Enum nl => EnumGraph nl el -> NonEmpty ((RangeStart, RangeLen), (nl, [el]))
  ranges graph = rangesE graph
  judyGraph graph = judyGraphE graph


-- | The enumGraph enumerates all child edges
--   and maps to the second 32 bit of the key of all nodeEdges
allChildEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                 EnumGraph nl el -> Node32 -> IO [Edge32]
allChildEdges jgraph (Node32 node) = map Edge32 <$> (allChilds jgraph (Node32 node) indexGen)
 where
  indexGen (base, count) | count < 1 = []
                         | otherwise = map f [0..(count-1)]
    where f x = base + x*2+1


-- | All adjacent child nodes
-- | The enumGraph enumerates all child edges
--   and maps to the second 32 bit of the key of all nodeEdges
allChildNodes :: (NodeAttribute nl, EdgeAttribute el) =>
                 EnumGraph nl el -> Node32 -> IO [Node32]
allChildNodes jgraph (Node32 node) = map Node32 <$> allChilds jgraph (Node32 node) indexGen
 where
  indexGen (base, count) | count < 1 = []
                         | otherwise = map f [0..(count-1)]
    where f x = base + x*2+2

allChilds :: (NodeAttribute nl, EdgeAttribute el) =>
                 EnumGraph nl el -> Node32 -> ((Word32,Word32) -> [Word32]) -> IO [Word32]
allChilds jgraph (Node32 node) indexGen = do
  edgeCounts <- mapM lu edgeCountKeys
  let ecs = map (fromMaybe 0) edgeCounts
  let indexes = concat (map indexGen (zip enumBases ecs))
  let enumKeys = map (buildWord64 node) indexes
  edges <- mapM (\key -> J.lookup key mj) enumKeys
  return ( -- Debug.Trace.trace ("ec " ++ show (node, enumKeys, edgeCounts)) $
           (catMaybes edges))
 where
  lu :: Word -> IO (Maybe Word32)
  lu key = J.lookup key mj
  enumBases = map fastEdgeAttrBase ((snd . snd . inRange node . toList . rangesE) jgraph)
  edgeCountKeys :: [Word]
  edgeCountKeys = map (buildWord64 node) enumBases
  mj = enumGraph jgraph


allAttrBases :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el) =>
                 EnumGraph nl el -> Node32 -> IO [Edge32]
allAttrBases jgraph (Node32 node) = do
  return (map Edge32 enumBases)
 where
  enumBases = -- Debug.Trace.trace ("bases "++ show (node, inRange node (toList (rangesE jgraph)))) $
              map fastEdgeAttrBase ((snd . snd . inRange node . toList . rangesE) jgraph)
--  mj = enumGraph jgraph


-- | To avoid the recalculation of edges
allChildNodesFromEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                          EnumGraph nl el -> Node32 -> [Edge32] -> IO [Node32]
allChildNodesFromEdges jgraph (Node32 node) edges = do
  let keys = map (\(Edge32 edge) -> buildWord64 node edge) edges
  nodes <- mapM (\key -> J.lookup key j) keys
  return (map Node32 -- Debug.Trace.trace ("ec " ++ show enumKeys) $
          (catMaybes nodes))
 where
  j = judyGraphE jgraph


inRange :: (NodeAttribute nl, EdgeAttribute el) =>
           Word32 -> [((Word32, Word32), (nl, [el]))] -> ((Word32, Word32), (nl, [el]))
inRange _ [] = error "node is not inRange, Enum.hs" -- should not happen
inRange _ [((start,len),(nl,els))] = ((start,len),(nl,els))
inRange node (((start,len),(nl,els)):xs) | node >= start = ((start,len),(nl,els))
                                         | otherwise = inRange node xs

emptyE :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
          NonEmpty ((RangeStart, RangeLen), (nl, [el])) -> IO (EnumGraph nl el)
emptyE rs = empty rs

fromListE :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
             Bool -> [(Node32, nl)] -> [(Edge, Maybe nl, Maybe nl, [el], Bool)]
                                    -> [(Edge, Maybe nl, Maybe nl, [el])] ->
             NonEmpty ((RangeStart, RangeLen), (nl, [el])) -> IO (EnumGraph nl el)
fromListE overwrite nodes dirEdges edges rs =
  fromList overwrite nodes dirEdges edges rs

-------------------------------------------------------------------------------------------

-- | The input format consists of lines where each line consists of two unsigned Ints 
-- (nodes), separated by whitespace
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
                   _ <- insertNodeEdge2 jgraph ((Node32 (fromIntegral n0),
                                                 Node32 (fromIntegral n1)),
                                                 Edge32 (snd (fastEdgeAttr edgeLabel)))
                   parse rest
                 else return ()

    readInts s = do (n0, s1) <- C.readInt s -- (Debug.Trace.trace ("0"++ [C.head s]) s)
                    let s2 =    C.dropWhile C.isSpace s1
                    (n1, s3) <- C.readInt s2
                    let s4 =    C.dropWhile C.isSpace s3
                    return ((n0, n1), s4)


-- | Faster version of insertNodeEdge that has no counter for multiple edges 
--   from the same origin to the destination node. There are also no node labels.
insertNodeEdge2 :: (NodeAttribute nl, EdgeAttribute el) =>
                   EnumGraph nl el -> (Edge, Edge32) -> IO (EnumGraph nl el)
insertNodeEdge2 jgraph ((Node32 n0, Node32 n1), Edge32 edgeAttr) = do
    let newValKey = buildWord64 n0 edgeAttr
    J.insert newValKey n1 (judyGraphE jgraph)
    return jgraph

-- | Keep a second judy array that points to all edges, nodes
--   Writes an edge to an edge and an edge to a node with every insert
insertEnumEdges :: (NodeAttribute nl, EdgeAttribute el) =>
                  EnumGraph nl el -> Node32 -> Node32 -> Edge32 -> Edge32 -> IO ()
insertEnumEdges jgraph (Node32 n0Key) (Node32 n1Key) (Edge32 attrBase) (Edge32 edgeAttr) = do

    edgeCount <- J.lookup edgeCountKey mj
    if isNothing edgeCount
          then J.insert edgeCountKey 1 mj -- the first edge is added, set counter to 1
          else J.insert edgeCountKey ((fromJust edgeCount)+1) mj -- inc counter by 1
    let enumKey0 = buildWord64 n0Key (attrBase+(fromMaybe 0 edgeCount)*2+1)
    let enumKey1 = buildWord64 n0Key (attrBase+(fromMaybe 0 edgeCount)*2+2)
    J.insert enumKey0 edgeAttr mj
    J.insert enumKey1 n1Key mj
--       (Debug.Trace.trace ("enEdge "++ show (n0Key, Edge32 (attrBase+(fromMaybe 0 edgeCount)+1), Node32 edgeAttr)) mj)
  where
    mj = enumGraph jgraph
    -- the enumgraph has the # of adjacent edges at index 0
    edgeCountKey = buildWord64 n0Key attrBase


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

-----------------------------------------------------------------------------------------
-- Query

-- | Useful if you want all adjacent edges, but you cannout check all 32 bit words with a
-- lookup and the distribution of the fast attributes follows no rule, there is no other
-- choice but to enumerate all edges. Eg you reserve 24 bit for a unicode char, but only
-- 50 chars are used. But some algorithms need all adjacent edges, for example merging
-- results of leaf nodes in a tree recursively until the root is reached
adjacentEdgesByIndex :: (NodeAttribute nl, EdgeAttribute el) =>
             EnumGraph nl el -> Node32 -> Edge32 -> Bool -> (Start, End) -> IO [Edge32]
adjacentEdgesByIndex jgraph (Node32 node) (Edge32 attrBase) dir (Node32 start, Node32 end) = do
    val <- J.lookup key mj -- (Debug.Trace.trace (show (node,Edge32 attrBase,dir,start,end)) mj)
    next <- if start <= end
            then adjacentEdgesByIndex jgraph (Node32 node) (Edge32 attrBase) dir
                                      (Node32 (start+1), Node32 end)
            else return []
    return (if isJust val then (Edge32 (fromJust val)) : next else next)
  where
    key = buildWord64 node (attrBase + (start*2+1) + if dir then 0 else edgeBackward)
    mj = enumGraph jgraph


-- | Nearly the same as adjacentEdgesByIndex, with the difference that a node follows to an edge
--   This interleaving of nodes and edges in the enum grapg had to be introduced because
--   sometimes edge+node is not unique and only edge+(node,node) is unique (The same starting 
--   node with the same edge points to several nodes).
adjacentNodesByIndex :: (NodeAttribute nl, EdgeAttribute el) =>
             EnumGraph nl el -> Node32 -> Edge32 -> Bool -> (Start, End) -> IO [Node32]
adjacentNodesByIndex jgraph (Node32 node) (Edge32 attrBase) dir (Node32 start, Node32 end) = do
    val <- J.lookup key mj -- (Debug.Trace.trace (show (node,Edge32 attrBase,dir,start,end)) mj)
    next <- if start <= end
            then adjacentNodesByIndex jgraph (Node32 node) (Edge32 attrBase) dir
                                      (Node32 (start+1), Node32 end)
            else return []
    return (if isJust val then (Node32 (fromJust val)) : next else next)
  where
    key = buildWord64 node (attrBase + (start*2+2) + if dir then 0 else edgeBackward)
    mj = enumGraph jgraph


---------------------------------------------------------------------------
-- | The number of adjacent edges
adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) =>
                     EnumGraph nl el -> Node32 -> Edge32 -> IO Word32
adjacentEdgeCount jgraph (Node32 node) (Edge32 attrBase) = do
    -- the first index lookup is the count
    let edgeCountKey = buildWord64 node attrBase -- (nodeWithMaybeLabel node nl) 0
    edgeCount <- J.lookup edgeCountKey mj
    return (fromMaybe 0 edgeCount)
  where
--    nl = maybe Nothing (Map.lookup node) (complexNodeLabelMap jgraph)
    mj = enumGraph jgraph

