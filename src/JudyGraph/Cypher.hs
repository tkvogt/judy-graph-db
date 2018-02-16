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
                  anyNode, nodes, edge
                 ) where

import qualified Data.Judy as J
import           Data.List(sortBy)
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing)
import qualified Data.Set as Set
import           Data.Set(Set)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import JudyGraph.FastAccess(JGraph(..), Judy(..), Node, NodeEdge, NodeAttribute (..),
                            EdgeAttribute(..), Bits(..), EdgeAttr, empty, nodesJ, hasNodeAttr,
                            allChildNodesFromEdges, allChildEdges, adjacentEdgesByAttr, buildWord64)
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
                      | CE Evaluated (CypherEdge nl el)

data (NodeAttribute nl, EdgeAttribute el) => CypherNode nl el =
  CypherNode { attr :: [Attr] -- ^Attribute: Highest bits of the 32 bit value
             , labels :: LabelNodes nl
             , restr :: Maybe (Node -> Maybe (Map Node nl) -> Bool)
             , cols0 :: [CypherComp nl el] -- ^In the beginning empty, grows by evaluating the query
             }

data (NodeAttribute nl, EdgeAttribute el) => CypherEdge nl el =
  CypherEdge { edgeAttrs :: [EAttr el] -- ^Attribute: Highest bits of the 32 bit value
             , edges :: Maybe [NodeEdge] -- ^Filled by evaluation
             , several :: Maybe (Int, Int)-- ^Variable number of edges tried for matching: eg "1..5"
             , edgeRestr :: Maybe (Word32 -> Maybe (Map (Node,Node) [el]) -> Bool)
             , cols1 :: [CypherComp nl el] -- ^In the beginning empty, gathers the components of the query
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
  -- * Nodes\/edges can be filtered with f by using the node\/edge index and the secondary data structure.
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
  where getW32 (EAttribute e) = snd (fastEdgeAttr e)
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
  (--|)  node (CypherEdge a e s r c) = CypherEdge a e s r ((CN False node):c)
  (|--)  edge (CypherNode a   l r c) = CypherNode a l r ((CE False edge):c)
  (<--|) node (CypherEdge a e s r c) =
               -- Assuming the highest bit is not set by someone else
               CypherEdge a e s r ((CN False node):c)
  (|-->) (CypherEdge a0 e s r0 c0)
         (CypherNode a1 l r1 c1) = CypherNode a1 l r1 ((CE False directedEdge):c1)
    where outEdgeAttr = EOrthogonal (fromJust edgeForward)
          directedEdge = CypherEdge (outEdgeAttr:a0) e s r0 c0
-- | isNothing edgeForward = error "You use |--> but haven't set edgeForward in EdgeLabel typeclass"

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryN (CypherNode nl el) where
  (-~-) node (CypherNode as ls r c) = CypherNode as ls r ((CN False node):c)
  (-->) node (CypherNode as ls r c) =
              CypherNode ((Orthogonal (bits, outgoing)):as) ls r ((CN False node):c)
    where bits = 1
  (<--) node (CypherNode a l r c) =
              -- Assuming the highest bit is not set by someone else
              CypherNode a l r ((CN False node):c)

data NE nl = N (LabelNodes nl) | NE [NodeEdge] deriving Show

class Table nl el a where
  -- | Evaluate the query to a table
  table :: JGraph nl el -> a -> IO [NE nl]

instance (NodeAttribute nl, EdgeAttribute el) =>
         Table nl el (CypherNode nl el) where
  -- table :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> CypherNode nl el -> IO [NE nl]
  table graph cypherNode = evalToTable graph (cols0 cypherNode)

instance (NodeAttribute nl, EdgeAttribute el) =>
         Table nl el (CypherEdge nl el) where
  -- table :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> CypherEdge nl el -> IO [NE nl]
  table graph cypherEdge = evalToTable graph (cols1 cypherEdge)

----------------------------------------------------------------------------------------------------

evalToTable :: (NodeAttribute nl, EdgeAttribute el) =>
               JGraph nl el -> [CypherComp nl el] -> IO [NE nl]
evalToTable graph comps = do res <- runOn graph (Map.fromList (zip [0..] comps))
                             return (map toNE (Map.elems res))
 where
  toNE (CN _ (CypherNode a ls r c)) = N ls
  toNE (CE _ (CypherEdge a (Just es) s r c)) = NE es
  toNE (CE _ (CypherEdge a Nothing s r c)) = NE []


data Compl a = NCompl a | ECompl a
fromC (NCompl a) = a
fromC (ECompl a) = a

-- | Estimating how much work it is to compute these elements
-- TODO use counter of edge-attr
compl (CN _ (CypherNode a AllNodes r c)) = NCompl 100000 -- Just a high number
compl (CN _ (CypherNode a (Label ls) r c)) = NCompl (length ls) -- Around 1 to 100 labels
compl (CN _ (CypherNode a (Nodes n)  r c)) = NCompl 0 -- Nodes contain a low number of (start) nodes
                                     -- 0 because a single label will mostly contain much more nodes
compl (CE _ (CypherEdge a e s r c)) = ECompl (length a)


minI n i min [] = i
minI n i min ((NCompl c):cs) | c < min   = minI (n+1) n c   cs
                             | otherwise = minI (n+1) i min cs
minI n i min ((ECompl c):cs) = minI (n+1) i min cs -- skip edges


isEval (CN evaluated _) = evaluated
isEval (CE evaluated _) = evaluated


evalNodes :: (NodeAttribute nl, EdgeAttribute el) =>
             JGraph nl el -> (CypherComp nl el) -> IO (CypherComp nl el)
evalNodes graph (CN _    (CypherNode a (Nodes n) r c)) =
         return (CN True (CypherNode a (Nodes n) r c))

evalNodes graph (CN _    (CypherNode a (AllNodes) r c)) = do
         allNodes <- nodesJ graph
         return (CN True (CypherNode a (Nodes allNodes) r c))

evalNodes graph (CN _    (CypherNode a (Label ls) r c)) = do
         lNodes <- filter hasOneOfTheLabels <$> nodesJ graph -- TODO make it faster
         return (CN True (CypherNode a (Nodes lNodes) r c))
  where hasOneOfTheLabels n = or (map (hasNodeAttr n) ls)

extractNodes (CN _ (CypherNode a (Nodes ns) r c)) = ns

-- allChildEdges :: JGraph nl el -> Node -> IO [EdgeAttr]
-- allChildNodesFromEdges :: JGraph nl el -> (Node, [EdgeAttr]) -> IO [Node]
-- adjacentEdgesByAttr :: (NodeAttribute nl, EdgeAttribute el) =>
--                       JGraph nl el -> Node -> EdgeAttr -> IO [EdgeAttr]
-- | This function repeatedly computes a column in the final table
--   until all columns are evaluated. The columns alternate from left to right between sets of
--   nodes or edges (enforced by the EDSL). The algorithm choses the node-column which is fastest
--   to compute. Then it choses the edge column left or right (together with the nodes where it 
--   leads) to pick the faster one of the two choices.
runOn :: (NodeAttribute nl, EdgeAttribute el) =>
         JGraph nl el -> Map Int (CypherComp nl el) -> IO (Map Int (CypherComp nl el))
runOn graph comps
  | all isEval (Map.elems comps) = return comps
  | otherwise =
    do evalCenter <- evalNodes graph (fromJust center)
       adjacentEdges <- mapM getEdges (extractNodes evalCenter)
       let es = adjacentEdges -- TODO apply WHERE restriction
       newNodes <- concat <$> mapM (allChildNodesFromEdges graph) es
       let ns = newNodes
       let restrictedNodes = CN True (CypherNode [] (Nodes ns) Nothing [])
       let restrictedEdges = CE True (CypherEdge [] (Just (concat (map nodeEdges es))) Nothing Nothing [])
       let newCenter = Map.insert minIndex evalCenter comps
       runOn graph (newComponents restrictedEdges restrictedNodes newCenter)
 where
  getEdges :: Node -> IO (Node, [EdgeAttr])
  getEdges n | null (edgeAttrs lOrR) = do ace <- allChildEdges graph n
                                          return (n, ace)
             | otherwise = (\x -> (n,x)) . concat <$> mapM (adjacentEdgesByAttr graph n) attrs
    where attrs = genAttrs (edgeAttrs leftEdges)
          lOrR | useLeft   = leftEdges
               | otherwise = rightEdges

  nodeEdges :: (Node, [EdgeAttr]) -> [NodeEdge]
  nodeEdges (n,as) = map (buildWord64 n) as

  computeComplexity = Map.map compl comps
  minIndex = minI 0 0 100000 (Map.elems computeComplexity)

  newComponents :: (CypherComp nl el) -> (CypherComp nl el) -> Map Int (CypherComp nl el)
                                                            -> Map Int (CypherComp nl el)
  newComponents es ns newCenter
      | useLeft   = Map.insert (minIndex-2) (ns)
                               (Map.insert (minIndex-1) (es) newCenter)
      | otherwise = Map.insert (minIndex+2) (ns)
                               (Map.insert (minIndex+1) (es) newCenter)

  useLeft | (isJust costLeft && isNothing costRight) ||
            (isJust costLeft && isJust costRight &&
                (fromC (fromJust costLeft) < fromC (fromJust costRight))) = True
          | otherwise = False
  costLeft  = Map.lookup (minIndex - 2) computeComplexity
  costRight = Map.lookup (minIndex + 2) computeComplexity

  center = Map.lookup minIndex comps
  leftEdges = unCE (fromJust (Map.lookup (minIndex - 1) comps))
  leftNodes = Map.lookup (minIndex - 2) comps
  rightEdges = unCE (fromJust (Map.lookup (minIndex + 1) comps))
  rightNodes = Map.lookup (minIndex + 2) comps

  unCN (CN _ x) = x
  unCE (CE _ x) = x

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

-- | Explicitly say which nodes to use, also restricted by inbounding edges
nodes ns = CypherNode [] (Nodes ns) Nothing []

-- | An empty edge. Apply Attributes and WHERE-restrictions on this.
edge :: (NodeAttribute nl, EdgeAttribute el) => CypherEdge nl el
edge = CypherEdge [] Nothing Nothing Nothing []

