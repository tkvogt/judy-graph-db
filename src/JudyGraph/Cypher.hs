{-# LANGUAGE OverloadedStrings, FlexibleInstances, MultiParamTypeClasses, FunctionalDependencies #-}
{-# LANGUAGE UnicodeSyntax #-} --, InstanceSigs, ScopedTypeVariables #-}
{-|
Module      :  Cypher
Description :  Cypher like edsl to make queries on the graph
Copyright   :  (C) 2017,2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

Neo4j invented Cypher as a sort of SQL for their graph database. But it is much nicer to have an 
EDSL in Haskell than a DSL that always misses a command. The ASCII art syntax could not be completely
 taken over, but it is very similar.
-}

module JudyGraph.Cypher(
         -- * Cypher Query
         QueryN(..), QueryNE(..),
         -- * Cypher Query with Unicode
         (─┤),  (├─),  (<─┤),  (├─>),  (⟞⟝), (⟼),  (⟻),
         -- * Query Components
         CypherComp(..), CypherNode(..), CypherEdge(..),
         -- * Query Evaluation
         Table(..), GraphCreateReadUpdate(..), NE(..),
         -- * Setting of Attributes, Labels,...
         SetAttr(..), AddLabel(..), Several(..), SetWHERE(..), AttrType(..), Attr(..), EAttr(..),
         LabelNodes(..),
         -- * Unevaluated node/edge markers
         anyNode, labels, nodes, edge
       ) where

import qualified Data.Judy as J
import           Data.List(sortBy, intercalate)
import           Data.List.NonEmpty(toList)
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing)
import qualified Data.Set as Set
import           Data.Set(Set)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import JudyGraph.FastAccess(JGraph(..), Judy(..), Node, NodeEdge, NodeAttribute (..),
                            EdgeAttribute(..), Bits(..), EdgeAttr, empty, hasNodeAttr,
                            allChildNodesFromEdges, allChildEdges, adjacentEdgesByAttr, buildWord64,
                            showHex, showHex32)
import Debug.Trace

----------------------------------------------------------------------
-- Query Classes

class QueryNE node edge where
  -- | Half of an undirected edge
  (--|)  :: node -> edge -> edge

  -- | Half of an undirected edge
  (|--)  :: edge -> node -> node

  -- | Half of a directed edge
  (<--|) :: node -> edge -> edge

  -- | Half of a directed edge
  (|-->) :: edge -> node -> node

class QueryN node where
  -- | An undirected edge
  (-~-) :: node -> node -> node

  -- | A directed edge
  (-->) :: node -> node -> node

  -- | A directed edge
  (<--) :: node -> node -> node

infixl 7  --|
infixl 7 |--
infixl 7 <--|
infixl 7 |-->
infixl 7 -~-
infixl 7 -->
infixl 7 <--

-----------------------------------------
-- More beatiful combinators with unicode

(─┤) x y = (--|) x y -- Unicode braces ⟬⟭⦃⦄⦗⦘ are not allowed
(├─) x y = (|--) x y
(<─┤) x y = (<--|) x y
(├─>) x y = (|-->) x y

(⟞⟝) x y = (-~-) x y
(⟼) x y = (-->) x y
(⟻) x y = (<--) x y

-- | How often to try an edge, eg 1…3
(…) n0 n1 = addSeveral n0 n1

infixl 7 ─┤
infixl 7 ├─
infixl 7 <─┤
infixl 7 ├─>
infixl 7  ⟞⟝
infixl 7  ⟼
infixl 7  ⟻

----------------------------------------------------------------------------------------------

data LabelNodes nl = AllNodes | Label [nl] | Nodes [Node] deriving Show

type Evaluated = Bool

-- | After evluating the query the result is a list of CypherComps, which have to be evaluated again
data CypherComp nl el = CN Evaluated (CypherNode nl el)
                      | CE Evaluated (CypherEdge nl el) deriving Show

data (NodeAttribute nl, EdgeAttribute el) => CypherNode nl el =
  CypherNode { attr :: [Attr] -- ^Attribute: Highest bits of the 32 bit value
             , labelsN :: LabelNodes nl
             , restr :: Maybe (Node -> Maybe (Map Node nl) -> Bool)
             , cols0 :: [CypherComp nl el] -- ^In the beginning empty, grows by evaluating the query
             }

data (NodeAttribute nl, EdgeAttribute el) => CypherEdge nl el =
  CypherEdge { edgeAttrs :: [EAttr el] -- ^Attribute: Highest bits of the 32 bit value
             , edges :: Maybe [NodeEdge] -- ^Filled by evaluation
             , several :: Maybe (Int, Int)-- ^Variable number of edges tried for matching: eg "1..5"
             , edgeRestr :: Maybe (Word32 -> Maybe (Map (Node,Node) [el]) -> Bool)
             , cols1 :: [CypherComp nl el] -- ^In the beginning empty, gathers the components of query
             }

data CypherNodeG nl el =
  CypherNodeG { attrG :: [Attr] -- ^Attribute: Highest bits of the 32 bit value
              , labelsG :: LabelNodes nl -- ^Take nodes of zero or more labels
              , restrG :: Maybe (Node -> Maybe (Map Node nl) -> Bool)
              , graphN :: JGraph nl el
              }

data CypherEdgeG nl el =
  CypherEdgeG { edgeAttrsG :: [EAttr el] -- ^Attribute: Highest bits of the 32 bit value
              , severalG :: Maybe (Int, Int)-- ^Variable number of edges tried for matching: eg "1..5"
              , edgeRestrG :: Maybe (Word32 -> Maybe (Map (Node,Node) [el]) -> Bool)
              , graphE :: JGraph nl el
              }

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherNode nl el) where 
  show (CypherNode _ _ _ _) = "CypherNode"

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherEdge nl el) where 
  show (CypherEdge _ _ _ _ _) = "CypherEdge"

data Attr =
     Attribute (Bits, Word32) -- ^Each attribute uses bits that are used only by this attribute
   | Orthogonal (Bits, Word32)-- ^Attributes can use all bits
   | FilterBy (Word32 -> Bool)-- ^Attributes are the result of filtering by a boolean expression


data EAttr el =
     EAttribute el -- ^Each attribute uses bits that are used only by this attribute
   | EOrthogonal el -- ^Attributes can use all bits
   | EFilterBy (Word32 -> Bool)-- ^Attributes are the result of filtering by a boolean expression


data AttrType = Attr
              | Orth
              | Filter (Word32 -> Bool)

----------------------------------------------------------------------------------------------
-- Attributes, Labels and WHERE-restrictions

class SetAttr l a where
  addAttr :: AttrType -> l -> a -> a

class NodeAttribute nodeLabel =>
      AddLabel nodeLabel node where
  -- | Nodes are in labeled partitions. That means that every node has only one label and every
  --   node index is in a range of a label. This can be used to interpret edges differently,
  --   depending in which range the node is.
  addLabel :: nodeLabel -> node -> node

class Several el where
  addSeveral :: Int -> Int -> el -> el

class SetWHERE f a where
  -- |
  -- * Nodes\/edges can be filtered with f by using the node\/edge index and the secondary 
  --   data structure.
  --
  -- * eg f :: Maybe (Node -> Maybe (Map Node nl) -> Bool)
  --
  -- * Similar to \"WHERE a.name == Alice\" in a SQL statement
  setWHERE :: f -> a -> a

instance (NodeAttribute nl, EdgeAttribute el) =>
         AddLabel nl (CypherNode nl el) where
  addLabel nl (CypherNode attr (Label ls) r c) = CypherNode attr (Label (nl : ls)) r c
  addLabel nl (CypherNode attr AllNodes r c)   = CypherNode attr (Label [nl]) r c
  addLabel nl (CypherNode attr nodes r c)      = CypherNode attr nodes r c

instance (NodeAttribute nl, EdgeAttribute el) =>
         SetAttr nl (CypherNode nl el) where
  addAttr Attr nl (CypherNode a l r c) = CypherNode ((Attribute  (fastNodeAttr nl)):a) l r c
  addAttr Orth nl (CypherNode a l r c) = CypherNode ((Orthogonal (fastNodeAttr nl)):a) l r c
--  addAttr (FilterBy f) nl (CypherNode _ l r c) = CypherNode (fastNodeAttr nl) l r c

instance (NodeAttribute nl, EdgeAttribute el) =>
         SetAttr el (CypherEdge nl el) where
  addAttr Attr el (CypherEdge a e s r c) = CypherEdge ((EAttribute  el):a) e s r c
  addAttr Orth el (CypherEdge a e s r c) = CypherEdge ((EOrthogonal el):a) e s r c
--  addAttr (FilterBy f) el (CypherEdge _ s r c) = CypherEdge (fastEdgeAttr el) s r c

instance (NodeAttribute nl, EdgeAttribute el) =>
         Several (CypherEdge nl el) where
  addSeveral n0 n1 (CypherEdge a e s r c) = (CypherEdge a e (Just (n0,n1)) r c)

instance (NodeAttribute nl, EdgeAttribute el) =>
         SetWHERE (Node -> Maybe (Map Node nl) -> Bool) (CypherNode nl el) where
  setWHERE f (CypherNode a l _ c) = CypherNode a l (Just f) c

instance (NodeAttribute nl, EdgeAttribute el) =>
         SetWHERE (Word32 -> Maybe (Map (Node,Node) [el]) -> Bool) (CypherEdge nl el) where
  setWHERE f (CypherEdge a e s _ c) = CypherEdge a e s (Just f) c

-- | TODO:
--  * (Ortho Vector0) X (Ortho Vector1) X (Attr0 + Attr1 + Attr2)
--
--  * X is Set Product, in this example there would be (2²-1) * 3 = 9 attributes
--
--  * Currently its just [Attr1,Attr2,Attr3]
genAttrs :: EdgeAttribute el => [EAttr el] -> [Word32]
genAttrs as = map getW32 attrs
  where getW32 (EAttribute e) = fastEdgeAttrBase e
        attrs = filter isAttr as
        orths = filter isOrtho as
        isOrtho (EOrthogonal _) = True
        isOrtho _ = False
        isAttr (EAttribute _) = True
        isAttr _ = False

----------------------------------------------------------------------------
-- Creating a table
outgoing = 0xf0000000 -- outgoing edge -- TODO check that this bit is unused

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryNE (CypherNode nl el) (CypherEdge nl el) where

  (--|) (CypherNode a0 l r0 c0) (CypherEdge a1 e s r1 c1)
           | null c0   = CypherEdge a1 e s r1 (ce:cn:c0)
           | otherwise = CypherEdge a1 e s r1 (   ce:c0)
                                where cn = CN False (CypherNode a0 l r0 c0)
                                      ce = CE False (CypherEdge a1 e s r1 c1)

  (|--) (CypherEdge a0 e s r0 c0) (CypherNode a1 l r1 c1)
           | null c0   = CypherNode a1 l r1 (cn:ce:c0)
           | otherwise = CypherNode a1 l r1 (   cn:c0)
                                where cn = CN False (CypherNode a1 l r1 c1)
                                      ce = CE False (CypherEdge a0 e s r0 c0)

               -- Assuming the highest bit is not set by someone else
  (<--|) (CypherNode a0 l r0 c0) (CypherEdge a1 e s r1 c1)
            | null c0   = CypherEdge a1 e s r1 (ce:cn:c0)
            | otherwise = CypherEdge a1 e s r1 (   ce:c0)
                                where ce = CE False (CypherEdge a1 e s r1 c1)
                                      cn = CN False (CypherNode a0 l r0 c0)

  (|-->) (CypherEdge a0 e s r0 c0) (CypherNode a1 l r1 c1)
            | null c0   = CypherNode a1 l r1 (cn:(CE False directedEdge):c0)
            | otherwise = CypherNode a1 l r1 (                        cn:c0)
    where outEdgeAttr = EOrthogonal (fromJust edgeForward)
          directedEdge = CypherEdge (outEdgeAttr:a0) e s r0 c0
          cn = CN False (CypherNode a1 l r1 c1)
-- | isNothing edgeForward = error "You use |--> but haven't set edgeForward in EdgeLabel typeclass"

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryN (CypherNode nl el) where
  (-~-) (CypherNode a0 l0 r0 c0) (CypherNode a1 l1 r1 c1)
                 | null c0   = CypherNode a1 l1 r1 (n1:n0:c0)
                 | otherwise = CypherNode a1 l1 r1 (   n1:c0)
                                where n0 = CN False (CypherNode a0 l0 r0 c0)
                                      n1 = CN False (CypherNode a1 l1 r1 c1)

  (-->) (CypherNode a0 l0 r0 c0) (CypherNode a1 l1 r1 c1)
              | null c0   = CypherNode a1 l1 r1 (n1:n0:c0)
              | otherwise = CypherNode a1 l1 r1 (   n1:c0)
                                where n0 = CN False (CypherNode a0 l0 r0 c0)
                                      n1 = CN False (CypherNode a1 l1 r1 c1)
                              -- ((Orthogonal (1, outgoing)):a0)

              -- Assuming the highest bit is not set by someone else
  (<--) (CypherNode a0 l0 r0 c0) (CypherNode a1 l1 r1 c1)
                | null c0   = CypherNode a1 l1 r1 (n1:n0:c0)
                | otherwise = CypherNode a1 l1 r1 (   n1:c0)
                                where n0 = CN False (CypherNode a0 l0 r0 c0)
                                      n1 = CN False (CypherNode a1 l1 r1 c1)

data NE nl = N (LabelNodes nl) | NE [NodeEdge]

instance Show (NE nl) where
  show (N (Nodes ns)) = "N " ++ show ns
  show (NE es) = "E [" ++ (intercalate "," (map showHex es)) ++ "]"

class Table nl el a where
  -- | Evaluate the query to a table
  table :: JGraph nl el -> a -> IO [NE nl]

instance (Eq nl, Show nl, NodeAttribute nl, EdgeAttribute el) =>
         Table nl el (CypherNode nl el) where
  -- table :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> CypherNode nl el -> IO [NE nl]
  table graph cypherNode
      | null (cols0 cypherNode) =
          do evalN <- evalNode graph (CypherNode [] (labelsN cypherNode) Nothing [])
             evalToTable graph [CN True evalN]
      | otherwise = evalToTable graph (reverse (cols0 cypherNode))

instance (Eq nl, Show nl, NodeAttribute nl, EdgeAttribute el) =>
         Table nl el (CypherEdge nl el) where
  -- table :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> CypherEdge nl el -> IO [NE nl]
  table graph cypherEdge | null (cols1 cypherEdge) = return []
                         | otherwise = evalToTable graph (reverse (cols1 cypherEdge))

----------------------------------------------------------------------------------------------------

evalToTable :: (Eq nl, Show nl, NodeAttribute nl, EdgeAttribute el) =>
               JGraph nl el -> [CypherComp nl el] -> IO [NE nl]
evalToTable graph comps = do res <- runOn graph (Map.fromList (zip [0..] comps))
                             return (map toNE (Map.elems res))
 where
  toNE (CN _ (CypherNode a ls r c)) = N ls
  toNE (CE _ (CypherEdge a (Just es) s r c)) = NE es
  toNE (CE _ (CypherEdge a Nothing s r c)) = NE []


data Compl a = NCompl a | ECompl a deriving Show

fromC (_, NCompl a) = a
fromC (_, ECompl a) = a

-- | Estimating how much work it is to compute these elements
-- TODO use counter of edge-attr
compl (CN eval (CypherNode a AllNodes r c))   = (eval, NCompl 100000) -- Just a high number
compl (CN eval (CypherNode a (Label ls) r c)) = (eval, NCompl (length ls)) -- Around 1 to 100 labels
compl (CN eval (CypherNode a (Nodes n)  r c)) = (eval, NCompl 0) -- Nodes contain a low number of (start) nodes
                                     -- 0 because a single label will mostly contain much more nodes
compl (CE eval (CypherEdge a e s r c))        = (eval, ECompl (length a))


minI n i min [] = i -- Debug.Trace.trace ("i " ++ show i) i
minI n i min ((True, _):cs) = -- Debug.Trace.trace ("eTrue " ++ show i)
                              (minI (n+1) i min cs) -- skip evaluated nodes

minI n i min ((False, NCompl c):cs) | c < min   = -- Debug.Trace.trace ("c < min"++ show i)
                                                  (minI (n+1) n c   cs)
                                    | otherwise = -- Debug.Trace.trace ("other " ++ show (i,c,min) )
                                                  (minI (n+1) i min cs)

minI n i min ((False, ECompl c):cs) = -- Debug.Trace.trace ("e " ++ show i)
                                      (minI (n+1) i min cs) -- skip edges

unEvalCount (CN evaluated _) = if evaluated then 0 else 1
unEvalCount (CE evaluated _) = if evaluated then 0 else 1


evalComp :: (Eq nl, Show nl, NodeAttribute nl, EdgeAttribute el) =>
             JGraph nl el -> (CypherComp nl el) -> IO (CypherComp nl el)
evalComp graph (CN _  c) = do n <- evalNode graph c
                              return (CN True n)

evalNode :: (Eq nl, Show nl, NodeAttribute nl, EdgeAttribute el) =>
             JGraph nl el -> (CypherNode nl el) -> IO (CypherNode nl el)
evalNode graph (CypherNode a (Nodes n) r c) =
        return (CypherNode a (Nodes n) r c)

evalNode graph (CypherNode a (AllNodes) r c) = do
        return (CypherNode a (Nodes [0..(nodeCount graph)]) r c)

evalNode graph (CypherNode a (Label ls) r c) = do
        return (CypherNode a (Nodes (Debug.Trace.trace (show lNodes) lNodes)) r c)
  where lNodes = concat (map f spans)
        f ((start,end), nl) | elem nl ls = [start..end]
                            | otherwise  = []
        rangeList = toList (ranges graph)
        rangeEnds = map (\x -> x-1) $ (tail (map fst rangeList)) ++ [nodeCount graph]
        spans = zipWith spanGen rangeList rangeEnds
        spanGen (r, nl) e = ((r,e), nl)

extractNodes (CN _ (CypherNode a (Nodes ns) r c)) = ns

-- | This function repeatedly computes a column in the final table
--   until all columns are evaluated. The columns alternate from left to right between sets of
--   nodes and sets of edges (enforced by the EDSL). The algorithm choses the node-column which is 
--   fastest to compute. Then it choses the edge column left or right (together with the nodes where 
--   it leads) to pick the faster one of the two choices.
runOn :: (Eq nl, Show nl, NodeAttribute nl, EdgeAttribute el) =>
         JGraph nl el -> Map Int (CypherComp nl el) -> IO (Map Int (CypherComp nl el))
runOn graph comps
  | unevaluatedNodes == 0 = return comps
  | unevaluatedNodes == 1 = if unEv (head elems) then return comps -- TODO edges to anynode
                                                 else return comps -- unEv (last elems)
  | otherwise =
    do evalCenter <- evalComp graph (fromJust center)
       adjacentEdges <- mapM getEdges (extractNodes evalCenter)
       let es = -- Debug.Trace.trace ("ee " ++ show adjacentEdges) $
                adjacentEdges -- TODO apply WHERE restriction
       newNodes <- concat <$> mapM (allChildNodesFromEdges graph) es
       let ns = -- Debug.Trace.trace ("nn " ++ show newNodes ++ show es)
                newNodes -- TODO: If already evaluated at this place,
                         -- choose smaller list and probably reduce node lists in this direction
       let centerNodesAdj = map fst (filter (\(_,x) -> not (null x)) es)
       let adjCenter = CN True (CypherNode [] (Nodes centerNodesAdj) Nothing [])
       let restrNodes = CN True (CypherNode [] (Nodes ns) Nothing [])
       let restrEdges = CE True (CypherEdge [] (Just (concat (map nodeEdges es))) Nothing Nothing [])
       let newCenter = Map.insert minIndex adjCenter comps
       runOn graph (newComponents restrEdges restrNodes newCenter)
 where
  unevaluatedNodes = sum (map unEvalCount elems)
  unEv (CE False e) = True
  unEv _ = False
  elems = Map.elems comps
  getEdges :: Node -> IO (Node, [EdgeAttr])
  getEdges n | null (edgeAttrs lOrR) = do ace <- allChildEdges graph n
                                          return (n, ace)
             | otherwise = (\x -> (n,x)) . concat <$> mapM (adjacentEdgesByAttr graph n) attrs -- (Debug.Trace.trace ("ac " ++ concat (map showHex32 attrs)) n)) attrs
    where attrs | useLeft   = genAttrs (edgeAttrs (fromJust leftEdges))
                | otherwise = genAttrs (edgeAttrs (fromJust rightEdges))
          lOrR | useLeft   = fromJust leftEdges
               | otherwise = fromJust rightEdges

  nodeEdges :: (Node, [EdgeAttr]) -> [NodeEdge]
  nodeEdges (n,as) = map (buildWord64 n) as

  computeComplexity = Map.map compl comps
  minIndex = -- Debug.Trace.trace ("elems " ++ show comps ++ (show (Map.elems computeComplexity)))
                               (minI 0 0 1000000 (Map.elems computeComplexity))

  costLeft  = Map.lookup (minIndex - 2) computeComplexity
  costRight = Map.lookup (minIndex + 2) computeComplexity

  leftNodes  =          Map.lookup (minIndex - 2) comps
  leftEdges  = unCE <$> Map.lookup (minIndex - 1) comps
  center     = -- Debug.Trace.trace ("minIndex " ++ show minIndex) $
               Map.lookup minIndex comps
  rightEdges = unCE <$> Map.lookup (minIndex + 1) comps
  rightNodes =          Map.lookup (minIndex + 2) comps

  unCN (CN _ x) = x
  unCE (CE _ x) = x

  newComponents :: (CypherComp nl el) -> (CypherComp nl el) -> Map Int (CypherComp nl el)
                                                            -> Map Int (CypherComp nl el)
  newComponents es ns newCenter
    | useLeft && isJust leftNodes = Map.insert (minIndex-2) ns (Map.insert (minIndex-1) es newCenter)
    | useLeft && isNothing leftNodes =                          Map.insert (minIndex-1) es newCenter
    | isJust rightNodes =           Map.insert (minIndex+2) ns (Map.insert (minIndex+1) es newCenter)
    | otherwise =                                               Map.insert (minIndex+1) es newCenter

  useLeft | (isJust costLeft && isNothing costRight) ||
            (isJust costLeft && isJust costRight &&
                (fromC (fromJust costLeft) < fromC (fromJust costRight))) ||
            (isJust leftEdges && isNothing rightEdges) = True
          | otherwise = False

----------------------------------------------------------------
-- Creating a graph -- TODO implement

class GraphCreateReadUpdate nl el a where
  graphQuery :: JGraph nl el -> a -> IO (JGraph nl el)

  -- a graph that already contains nodes with labels
  create :: JGraph nl el -> a -> IO (JGraph nl el)

instance (NodeAttribute nl, EdgeAttribute el) =>
         GraphCreateReadUpdate nl el (CypherNodeG nl el) where
  graphQuery gr cypherNode = return (graphN cypherNode)
  create gr cypherNode = return gr

instance (NodeAttribute nl, EdgeAttribute el) =>
         GraphCreateReadUpdate nl el (CypherEdgeG nl el) where
  graphQuery gr cypherEdge = return (graphE cypherEdge)
  create gr cypherEdge = return gr

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryNE (CypherNodeG nl el) (CypherEdgeG nl el) where
  (--|) node edge = edge
  (|--) edge node = node
  (<--|) node edge = edge
  (|-->) edge node = node

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryN (CypherNodeG nl el) where
  (-~-) node0 node1 = node0
  (-->) node0 node1 = node0
  (<--) node0 node1 = node0

-------------------------------------------------------------------
-- | All nodes, but most likely restricted by which nodes are hit by inbounding edges
anyNode :: (NodeAttribute nl, EdgeAttribute el) => CypherNode nl el
anyNode  = CypherNode [] AllNodes Nothing []

labels ls = CypherNode [] (Label ls) Nothing []

-- | Explicitly say which nodes to use, also restricted by inbounding edges
nodes ns = CypherNode [] (Nodes ns) Nothing []

-- | An empty edge. Apply Attributes and WHERE-restrictions on this.
edge :: (NodeAttribute nl, EdgeAttribute el) => CypherEdge nl el
edge = CypherEdge [] Nothing Nothing Nothing []

