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

This module contains functions that use the lmdb databases 'nodeLabelDB' and
'edgeLabelDB' for persistent node/edge-labels that don't fit into 32 bit,
fill memory too quickly or need to be permanently stored.
You only have to write class instances that specify how node and edge labels are
converted into 32 bit labels: 'NodeAttribute', 'EdgeAttribute'. This shrinking obviously 
has to keep the information that is needed to quickly query the graph.
E.g. if you have e.g. 100.000 edges adjacent to a node and you
don't want to test them all for a certain label.

If you want to save memory and insertion-time and
if you can put each node/edge-label in less than 32 bit
then use only functions in the "Graph.FastAccess" module

If on the other hand there is enough space and the nodes or edges have much more info 
then the functions in this module generate fast access node-edges with the 
"Graph.FastAccess" module.

-}
module JudyGraph (JGraph(..), EnumGraph(..), Judy, Node32(..), Edge32(..), Edge,
      -- * Construction
      PersistentGraph(..), fromListE, fromDB, listToDB, emptyDB,
      insertCSVEdgeStream,
      insertNode, insertNodes, insertNodeLines, insertNodeEdge, insertNodeEdges, union,
      -- * Deletion
      deleteNode, deleteNodes, deleteEdge, deleteEdges,
      -- * Query
      isNull, lookupNode, lookupEdge, adjacentEdgeCount, nodeElems, nodeKeys,
      -- * Changing node labels
      mapNodeJ, mapNodeWithKeyJ,
      -- * Cypher Query
      QueryN(..), QueryNE(..),
      -- * Cypher Query with Unicode
      (─┤),  (├─),  (<─┤),  (├─>),  (⟞⟝), (⟼),  (⟻),
      -- * Query Components
      CypherComp(..), CypherNode(..), CypherEdge(..), appl,
      -- * Query Evaluation
      GraphCreateReadUpdate(..), evalNode,
      -- * Attributes, Labels,...
      EAttr(..), LabelNodes(..), NE(..),
      -- * type classes for translation into bits
      NodeAttribute(..), EdgeAttribute(..),
      -- * Node specifiers
      node, anyNode, labels, nodes32, NodeAttr(..), NAttr(..),
      -- * Edge specifiers
      edge, attr, orth, where_, several, (…), (***), genAttrs, extractVariants, AttrVariants(..),
      -- * Helpers
      n32n, n32e
     ) where

import           Codec.Serialise
import           Control.Monad(foldM, liftM)
import           Data.Binary(Binary)
import qualified Data.Binary as Bin
import qualified Data.ByteString as B
import           Data.Char (chr)
import           Data.Function((&))
import qualified Data.Judy as J
import           Data.List.NonEmpty(NonEmpty(..))
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(maybe, fromMaybe, fromJust)
import           Data.Text(Text)
import qualified Data.Text as T
import           Data.Time
import           Data.Vector(Vector)
import           Data.Word(Word32)
import           Database.LMDB.Simple
import           Database.LMDB.Simple.Extra (keys, elems)
import           System.IO.Unsafe(unsafePerformIO)

import JudyGraph.Enum(GraphClass(..), JGraph(..), EnumGraph(..), Judy,
                  NodeAttribute(..), EdgeAttribute(..), Edge32(..), Node32(..), Edge,
                  RangeStart, RangeLen, isNull, fromListE,
                  insertCSVEdgeStream, buildWord64, insertNodeEdges, insertNodeLines, edgeBackward,
                  deleteNode, deleteEdge, union, mapNodeJ, mapNodeWithKeyJ, lookupJudyNodes)
import JudyGraph.Cypher
import Control.Exception
import Network.ByteOrder

-- import System.ProgressBar
-- import Debug.Trace

-- | If the graph contains data that doesn't fit into 32 bit node/edges and there is
--   enough memory
data (NodeAttribute nl, EdgeAttribute el) =>
     PersistentGraph nl el = PersistentGraph {
  judyGraphC :: Judy, -- ^ A Graph with 32 bit keys on the edge
  enumGraphC :: Judy, -- ^ Enumerate the edges of the first graph,
                      --   with counter at position 0.
                     --   Deletions in the first graph are not updated here (too costly)
  nodeLabelDB :: Database Node32 nl, -- ^ A node attr that doesn't fit into 64bit
  edgeLabelDB :: Database (Node32,Node32) [el], -- Maybe (Map (Node32,Node32) [el]),
  rangesC :: NonEmpty ((RangeStart, RangeLen), (nl, [el])), -- ^ a nonempty list with an attribute
                                                            --   for every range
  nodeCountC :: Word32,
  dbEnvironment :: Environment ReadWrite,
  dbLocation :: FilePath,
  dbLimits :: Limits -- limits = Limits 1000000000 10 126
}

instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
         Show (PersistentGraph nl el) where
  show (PersistentGraph j e _ _ r _ _ _ _) = "(judyGraphC, enumGraphC) " ++ show (unsafePerformIO (J.size j), unsafePerformIO (J.size e))
                                             -- ++ show (unsafePerformIO (J.keys jfr), unsafePerformIO (J.keys efr))
    where jfr = unsafePerformIO (J.freeze j)
          efr = unsafePerformIO (J.freeze e)

instance Binary Node32 where
    {-# INLINE put #-}
    put (Node32 w32)           = Bin.put w32
    {-# INLINE get #-}
    get                 = liftM Node32 Bin.get

-- | Inserting a new node means either
--
--  * if its new then only add an entry to the secondary data.map
--
--  * if it already exists then the label of the existing node is changed.
--    This can be slow because all node-edges have to be updated (O(#adjacentEdges))
insertNode :: (NodeAttribute nl, EdgeAttribute el, Serialise nl, Binary nl) =>
              PersistentGraph nl el -> (Node32, nl) -> IO (PersistentGraph nl el)
insertNode (PersistentGraph j eg _ em r c dbEnv dbLoc dbLim) (n, nl) = do
  db <- readWriteTransaction dbEnv $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
  transaction dbEnv $ put db n (Just nl)

--  let newNodeAttr = maybe Map.empty (Map.insert node nl) nm
  -- the first index lookup is the count
--  let enumNodeEdge = buildWord64 (nodeWithLabel node nl) 0
--  numEdges <- J.lookup enumNodeEdge eg
--  when (isJust numEdges) $ do
--       es <- allChildEdges (EnumGraph j eg r n) node
            -- :: (NodeAttribute nl, EdgeAttribute el) => IO [EdgeAttr32]
--       ns <- allChildNodesFromEdges (EnumGraph j eg r n :: (NodeAttribute nl, EdgeAttribute el) => EnumGraph nl el) node es
--       mapM_ (updateNodeEdges (JGraph j r n) node nl) [] -- (zip es ns)
  return (PersistentGraph j eg db em r c dbEnv dbLoc dbLim)
-- where
--  ace = allChildEdges (EnumGraph j eg r n) node

-- | Insert several nodes using 'insertNode'
insertNodes :: (NodeAttribute nl, EdgeAttribute el, Serialise nl, Binary nl) =>
               PersistentGraph nl el -> [(Node32, nl)] -> IO (PersistentGraph nl el)
insertNodes graph nodes = do
  foldM insertNode graph nodes


emptyDB :: (NodeAttribute nl, EdgeAttribute el) =>
           NonEmpty ((RangeStart, RangeLen), (nl, [el])) -> FilePath -> Limits -> IO (PersistentGraph nl el)
emptyDB rs dbLoc dbLim = do -- TODO clear db?
    j <- J.new :: IO Judy
    e <- J.new :: IO Judy
    env <- openReadWriteEnvironment dbLoc dbLim
    nlmap <- readWriteTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
    elmap <- readWriteTransaction env $ getDatabase (Just "edgeLabelDB") :: IO (Database (Node32,Node32) [el])
    return (PersistentGraph j e nlmap elmap rs 0 env dbLoc dbLim)

-- | If you don't need persistent node/edge labels use 'fromListJ'
-- Don't use this if list is big
listToDB :: (NodeAttribute nl, Binary nl, Binary el, EdgeAttribute el, Show nl, Show el, Serialise nl, Serialise el, Enum nl) =>
            Bool -> [(Node32, nl)]
                 -> [((Node32, Node32), Maybe nl, Maybe nl, [el], Bool)]
                 -> [((Node32, Node32), Maybe nl, Maybe nl, [el])]
                 -> NonEmpty ((RangeStart, RangeLen), (nl, [el]))
                 -> FilePath
                 -> Limits
                 -> IO (PersistentGraph nl el)
listToDB overwrite nodes directedEdges es rs dbLoc dbLim = do
    putStrLn "fromList"
    jgraph <- emptyDB rs dbLoc dbLim
    putStrLn "fromList 1"
    start <- getCurrentTime
    ngraph <- insertNodes jgraph nodes
    end <- getCurrentTime
    print (diffUTCTime end start)
    putStrLn "fromList judy"
    insertNodeEdges overwrite ngraph nodes
                    (directedEdges ++ (map addDir es) ++ (map dirRev es) )
  where addDir ((from,to), nl0, nl1, labls) = ((from,to), nl1, nl0, labls, True)
        dirRev ((from,to), nl0, nl1, labls) = ((to,from), nl1, nl0, labls, True)


fromDB :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl, Serialise nl) =>
          Bool -> NonEmpty ((RangeStart, RangeLen), (nl, [el])) -> FilePath -> Limits -> IO (PersistentGraph nl el)
fromDB _ rs dbLoc dbLim = do -- TODO
    j <- J.new :: IO Judy
    e <- J.new :: IO Judy
    env <- openReadWriteEnvironment dbLoc dbLim
    nlmap <- readWriteTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
    elmap <- readWriteTransaction env $ getDatabase (Just "edgeLabelDB") :: IO (Database (Node32,Node32) [el])
    return (PersistentGraph j e nlmap elmap rs 0 env dbLoc dbLim)

instance (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl, Serialise nl, Serialise el, Binary nl, Binary el) =>
         GraphClass PersistentGraph nl el where

  -- | Are the judy arrays and nodeLabelMap and edgeLabelMap empty
  isNull :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
            PersistentGraph nl el -> IO Bool
  isNull (PersistentGraph graph enumGr _ _ rs n _ _ _) = do
    isN <- isNull (EnumGraph graph enumGr rs n edgeFromAttr ::
                       (NodeAttribute nl, EdgeAttribute el) => EnumGraph nl el)
    return (isN) -- && (maybe True Map.null nodeLabelMap)
                 -- && (maybe True Map.null edgeLabelMap))

  addNodeCount nodes gr = gr { nodeCountC = (nodeCountC gr) + fromIntegral (length nodes) }

  insertNodeEdges overwrite jgraph nodes es = fmap (addNodeCount nodes) (foldM foldEs jgraph es)
    where
      foldEs g ((n0, n1), nl0, nl1, edgeLs, dir) = fmap fst $ insertNodeEdgeAttr overwrite g e
        where e = ((n0, n1), nl0, nl1, overlay edgeLs, overlay edgeLs)
              overlay el = Edge32 (sum (map (addDir . snd . fastEdgeAttr) el))
              addDir attribute | dir = attribute
                               | otherwise = attribute + edgeBackward

  insertNodeEdge overwrite jgraph ((n0,n1), _, _, edgeLabels, dir) = do
    nl0 <- lookupNode jgraph n0
    nl1 <- lookupNode jgraph n1
    oldLabel <- lookupEdge jgraph (n0,n1)
    db <- readWriteTransaction (dbEnvironment jgraph) $ getDatabase (Just "edgeLabelDB") :: IO (Database (Node32,Node32) [el])
    transaction (dbEnvironment jgraph) $ put db (n0,n1) (Just ((fromMaybe [] oldLabel) ++ [edgeLabels]))

    _ <- insertNodeEdge overwrite jgraph ((n0, n1), nl0, nl1, edgeLabels, dir)
    return (jgraph) -- { edgeLabelDB = newEdgeLabelDB })


  insertNodeEdgeAttr overwrite jgraph ed = do
    (gr, res) <- insertNodeEdgeAttr overwrite egraph ed
    return (jgraph { judyGraphC = judyGraphE gr,
                     enumGraphC = enumGraph gr,
                     rangesC    = rangesE gr,
                     nodeCountC = nodeCountE gr
                   }, res)
   where egraph = (EnumGraph (judyGraphC jgraph) (enumGraphC jgraph)
                             (rangesC jgraph) (nodeCountC jgraph)) edgeFromAttr :: EnumGraph nl el

  insertCSVEdgeStream jgraph file newEdge = do
    nlmap <- readWriteTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
    elmap <- readWriteTransaction env $ getDatabase (Just "edgeLabelDB") :: IO (Database (Node32,Node32) [el])

    gr <- insertCSVEdgeStream egraph file (newNewEdge nlmap elmap)
    return (jgraph { judyGraphC = judyGraphE gr,
                     enumGraphC = enumGraph gr,
                     rangesC    = rangesE gr,
                     nodeCountC = nodeCountE gr })
   where
     env = dbEnvironment jgraph
     loc = dbLocation jgraph
     lim = dbLimits jgraph
     newNewEdge :: Database Node32 nl -> Database (Node32,Node32) [el] -> EnumGraph nl el -> [Text] -> IO (EnumGraph nl el)
     newNewEdge nm em gr strs = do
       newgr <- newEdge (PersistentGraph (judyGraphE gr) (enumGraph gr) nm em (rangesE gr) (nodeCountE gr) env loc lim) strs
       return (EnumGraph (judyGraphC newgr) (enumGraphC newgr)
                         (rangesC newgr) (nodeCountC newgr) edgeFromAttr)
--       where newgr = newEdge (PersistentGraph (judyGraphE gr) (enumGraph gr) nm em (rangesE gr) (nodeCountE gr) env loc lim) strs
     egraph = (EnumGraph (judyGraphC jgraph) (enumGraphC jgraph)
                         (rangesC jgraph) (nodeCountC jgraph) edgeFromAttr) :: EnumGraph nl el

  insertCSVEdge newEdge g (Right edgeProp) = newEdge g edgeProp
  insertCSVEdge _       g (Left _)         = return g

  ---------------------------------------------------------------------------------------
  -- Deletion
  -- Caution: Should currently be used much less than insert.
  --          It can make the lookup slower because of lookup failures

  -- | This will currently produce holes in the continuously enumerated edge list of
  --   enumGraph. But garbage collecting this is maybe worse.
  deleteNode :: (NodeAttribute nl, EdgeAttribute el, Serialise nl, Binary nl) =>
                PersistentGraph nl el -> Node32 -> IO (PersistentGraph nl el)
  deleteNode graph n = do
    db <- readWriteTransaction (dbEnvironment graph) $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
    transaction (dbEnvironment graph) $ put db n Nothing
    return graph

--    let newNodeMap = fmap (Map.delete node) (persistentNodeLabelMap jgraph)
--    deleteNode jgraph node
--    return (jgraph{ nodeLabelDB = newNodeMap })
--   where
--    nl = nodeWithMaybeLabel node (maybe Nothing (Map.lookup node) lmap)
--    mj = judyGraphC jgraph
--    lmap = persistentNodeLabelMap jgraph


  -- | This will currently produce holes in the continuously enumerated edge list of
  --   enumGraph. But garbage collecting this is maybe worse.
  deleteNodes graph nodes = do
--    env <- openReadWriteEnvironment (dbLocation graph) (dbLimits graph)
    newNodeDB <- foldM deleteNode graph nodes
    return (graph{ nodeLabelDB = nodeLabelDB newNodeDB })


  deleteEdges graph es = do
--    env <- openReadWriteEnvironment (dbLocation graph) (dbLimits graph)
    newEdgeDB <- foldM deleteEdge graph es
    return (graph{ edgeLabelDB = edgeLabelDB newEdgeDB })

  -- | This will currently produce holes in the continuously enumerated edge list of
  --   enumGraph. But garbage collecting this is maybe worse.
  deleteEdge :: (NodeAttribute nl, Binary nl, Binary el, EdgeAttribute el, Serialise el, Binary el) =>
                (PersistentGraph nl el) -> Edge -> IO (PersistentGraph nl el)
  deleteEdge (PersistentGraph j e nm em r n dbEnv dbLoc dbLim) (n0,n1) = do
    db <- readWriteTransaction dbEnv $ getDatabase (Just "edgeLabelDB") :: IO (Database (Node32,Node32) [el])
    transaction dbEnv $ put db (n0,n1) Nothing
    return (PersistentGraph j e nm em r n dbEnv dbLoc dbLim)

  --------------------------------------------------------------------------------------

  -- | Make a union of two graphs by making a union of 'persistentNodeLabelMap' and 
  --   'persistentEdgeLabelMap' but also calls 'unionJ' for a union of two judy arrays
  union :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, Enum nl) =>
           PersistentGraph nl el -> PersistentGraph nl el -> IO (PersistentGraph nl el)
  union (PersistentGraph j0 enumJ0 nodeLabelDB0 edgeLabelDB0 ranges0 n0 dbEnvironment0 dbLoc0 dbLim0)
        (PersistentGraph j1 enumJ1 nodeLabelDB1 edgeLabelDB1 ranges1 n1 _ _ _) = do

    (EnumGraph newJGraph newJEnum _ _ _ :: EnumGraph nl el)
        <- union (EnumGraph j0 enumJ0 ranges0 n0 edgeFromAttr)
                 (EnumGraph j1 enumJ1 ranges1 n1 edgeFromAttr)

    return (PersistentGraph newJGraph newJEnum
            (mapUnion nodeLabelDB0 nodeLabelDB1)
            (mapUnion edgeLabelDB0 edgeLabelDB1)
            ranges0 -- assuming ranges are global
            (n0+n1) dbEnvironment0 dbLoc0 dbLim0) -- TODO
   where
    mapUnion labelDB0 _ = labelDB0 -- TODO (Map.union labelDB0 labelDB1)

----------------------------------------------------------------------------------------

  adjacentEdgesByAttr jgraph (Node32 n32) (Edge32 attribute) = do
    n <- fmap (fmap Node32) (J.lookup key j)
    map fst <$> maybe (return [])
                      (lookupJudyNodes j (Node32 n32) (Edge32 attribute) True (Node32 1)) n
   where
    key = buildWord64 n32 attribute
    j = judyGraphC jgraph


--  allChildEdges jgraph node = do
--    return []


  filterEdgesTo jgraph nodeEdges f = do
    values <- mapM (\n -> fmap (fmap Edge32) (J.lookup n j)) nodeEdges
    return (map fst (filter filterNode (zip nodeEdges values)))
   where j = judyGraphC jgraph
         filterNode (_, Just v) | f v = True
                                | otherwise = False
         filterNode _ = False


  nodeCount graph = nodeCountC graph
  ranges :: Enum nl => PersistentGraph nl el -> NonEmpty ((RangeStart,RangeLen), (nl, [el]))
  ranges graph = rangesC graph
  judyGraph graph = judyGraphC graph

---------------------------------------------------------------------------------------
-- Changing node labels
{-
-- | This function only works on the lmdb for node labels
-- You have to figure out a function (Word32 -> Word32) that is equivalent to (nl -> nl)
-- and call mapNodeJ (so that everything stays consistent)
mapNode :: (NodeAttribute nl, EdgeAttribute el) =>
           (nl -> nl) -> PersistentGraph nl el -> IO (PersistentGraph nl el)
mapNode _ graph = do
  env <- openReadWriteEnvironment (dbLocation graph) (dbLimits graph)
  db <- readWriteTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
  return graph
-- TODO a way to iterate over all nodes: extend lmdb-simple with first, next
--  transaction env $
--    do c <- get db t
--       put dbCount 


--  let newMap = fmap (Map.map f) (nodeLabelDB jgraph)
--  return (jgraph { nodeLabelDB = newMap})

-- | This function only works on the lmdb for node labels
-- You have to figure out a function (Node- > Word32 -> Word32) that is equivalent 
-- to (Node -> nl -> nl) and call mapNodeWithKeyJ (so that everything stays consistent)
mapNodeWithKey :: (NodeAttribute nl, EdgeAttribute el) =>
                  (Node32 -> nl -> nl) -> (PersistentGraph nl el) -> IO (PersistentGraph nl el)
mapNodeWithKey _ jgraph = do
  return jgraph
-- TODO a way to iterate over all nodes: extend lmdb-simple with first, next
--  let newMap = fmap (Map.mapWithKey f) (nodeLabelDB jgraph)
--  return (jgraph { nodeLabelDB = newMap})
-}
------------------------------------------------------------------------------------------
-- Query

-- | This function only works on 'complexNodeLabelMap'
lookupNode :: (NodeAttribute nl, Binary nl, EdgeAttribute el, Serialise nl) =>
              PersistentGraph nl el -> Node32 -> IO (Maybe nl)
lookupNode graph n = do
  db <- readOnlyTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
  readOnlyTransaction env $ get db n
 where env = dbEnvironment graph

-- | This function only works on 'complexEdgeLabelMap'
lookupEdge :: (NodeAttribute nl, Binary nl, Binary el, EdgeAttribute el, Serialise el) =>
              PersistentGraph nl el -> Edge -> IO (Maybe [el])
lookupEdge graph (n0,n1) = do
  db <- readOnlyTransaction env $ getDatabase (Just "edgeLabelDB") :: IO (Database (Node32,Node32) [el])
  readOnlyTransaction env $ get db (n0,n1)
 where env = dbEnvironment graph

nodeElems :: (NodeAttribute nl, EdgeAttribute el, Serialise nl, Binary nl, Serialise el) => PersistentGraph nl el -> IO [nl]
nodeElems graph = do
  db <- readOnlyTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
  readOnlyTransaction env $ elems db
 where env = dbEnvironment graph

nodeKeys :: (NodeAttribute nl, EdgeAttribute el, Serialise nl) => PersistentGraph nl el -> IO [Node32]
nodeKeys graph = do
  db <- readOnlyTransaction env $ getDatabase (Just "nodeLabelDB") :: IO (Database Node32 nl)
  readOnlyTransaction env $ keys db
 where env = dbEnvironment graph

---------------------------------------------------------------------------
-- | The number of adjacent edges
adjacentEdgeCount :: (NodeAttribute nl, EdgeAttribute el) =>
                     PersistentGraph nl el -> Node32 -> Edge32 -> IO Word32
adjacentEdgeCount graph (Node32 n) (Edge32 attrBase) = do
    -- the first index lookup is the count
    let edgeCountKey = buildWord64 n attrBase -- (nodeWithMaybeLabel node nl) 0
    edgeCount <- J.lookup edgeCountKey mj
    return (fromMaybe 0 edgeCount)
  where
    mj = enumGraphC graph

------------------------------------------------------------------------------------------

instance (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el, Serialise nl, Serialise el, Binary nl, Binary el) =>
         GraphCreateReadUpdate PersistentGraph nl el (CypherNode nl el) where
  table graph _ cypherNode
    | null (cols0 cypherNode) =
      do (CypherNode a n _) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
         evalToTableC graph [CN (CypherNode a n True)]
    | otherwise = evalToTableC graph (reverse (cols0 cypherNode))

  temp graph _ cypherNode
    | null (cols0 cypherNode) =
      do (CypherNode a n _) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
         return [CN (CypherNode a n False)]
    | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                       (runOnC graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  createMem graph cypherNode
      | null (cols0 cypherNode) = return (GraphDiff [] [] [] []) -- TODO
      | otherwise = fmap snd (runOnC graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  graphQuery graph quickStrat cypherNode
      | null (cols0 cypherNode) =
          do (CypherNode a c _) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             if quickStrat then qEvalToGraphC graph [CN (CypherNode a c True)]
                           else  evalToGraphC graph [CN (CypherNode a c True)]
      | quickStrat = qEvalToGraphC graph (reverse (cols0 cypherNode))
      | otherwise  =  evalToGraphC graph (reverse (cols0 cypherNode))

  graphCreate gr _ = return gr -- cypherNode

instance (Eq nl, Show nl, Show el, NodeAttribute nl, Enum nl, EdgeAttribute el, Serialise nl, Serialise el, Binary nl, Binary el) =>
         GraphCreateReadUpdate PersistentGraph nl el (CypherEdge nl el) where
  table graph _ cypherEdge
      | null (cols1 cypherEdge) = return []
      | otherwise = evalToTableC graph (reverse (cols1 cypherEdge))

  temp graph _ cypherEdge
      | null (cols1 cypherEdge) = return []
      | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                         (runOnC graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  createMem graph cypherEdge
      | null (cols1 cypherEdge) = return (GraphDiff [] [] [] [])
      | otherwise = fmap snd (runOnC graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  graphQuery graph quickStrat cypherEdge
      | null (cols1 cypherEdge) = emptyDB (rangesC graph) (dbLocation graph) (dbLimits graph)
      | quickStrat = qEvalToGraphC graph (reverse (cols1 cypherEdge))
      | otherwise  =  evalToGraphC graph (reverse (cols1 cypherEdge))

  graphCreate gr _ = return gr --   graphCreate gr cypherEdge = return gr

-- TODO
evalToTableC :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                PersistentGraph nl el -> [CypherComp nl el] -> IO [NE nl]
evalToTableC _ _ = return []

-- TODO
runOnC :: (Eq nl, Show nl, Show el, Enum nl,
          NodeAttribute nl, EdgeAttribute el) =>
         PersistentGraph nl el -> Bool -> GraphDiff ->
         Map Int (CypherComp nl el) -> IO (Map Int (CypherComp nl el), GraphDiff)
runOnC graph create (GraphDiff dns newns des newEs) comps = -- Debug.Trace.trace ("rangesC "++ show (rangesC graph)) $
  runOnE egraph create (GraphDiff dns newns des newEs) comps
  where egraph = EnumGraph (judyGraphC graph) (enumGraphC graph) (rangesC graph) (nodeCountC graph) edgeFromAttr


-------------------------------------------------------------------------------
-- Creating a graph TODO

evalToGraphC :: (Eq nl, Show nl, Show el, Enum nl,
                NodeAttribute nl, EdgeAttribute el, GraphClass graph nl el) =>
                graph nl el -> [CypherComp nl el] -> IO (graph nl el)
evalToGraphC graph _ =
  do return graph

qEvalToGraphC :: (Eq nl, Show nl, Show el, Enum nl,
                NodeAttribute nl, EdgeAttribute el, GraphClass graph nl el) =>
                graph nl el -> [CypherComp nl el] -> IO (graph nl el)
qEvalToGraphC graph _ =
  do return graph

