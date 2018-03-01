{-# LANGUAGE OverloadedStrings, FlexibleInstances, MultiParamTypeClasses, FunctionalDependencies #-}
{-# LANGUAGE UnicodeSyntax, TypeOperators, GeneralizedNewtypeDeriving, AllowAmbiguousTypes #-}
-- {-# TypeSynonymInstances, ScopedTypeVariables #-}
--, InstanceSigs, ScopedTypeVariables #-}
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
         NAttr(..), Attr(..), edge, node, attr, where_, several, (…),
         LabelNodes(..),
         -- * Unevaluated node/edge markers
         anyNode, labels, nodes32
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
                            EdgeAttribute(..), Bits(..), EdgeAttr32, empty, hasNodeAttr,
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
(…) n0 n1 = several n0 n1

infixl 7 ─┤
infixl 7 ├─
infixl 7 <─┤
infixl 7 ├─>
infixl 7  ⟞⟝
infixl 7  ⟼
infixl 7  ⟻

----------------------------------------------------------------------------------------------

data LabelNodes nl = AllNs | Lbl [nl] | Ns [Node] deriving Show

type Evaluated = Bool

-- | After evluating the query the result is a list of CypherComps, which have to be evaluated again
data CypherComp nl el = CN Evaluated (CypherNode nl el)
                      | CE Evaluated (CypherEdge nl el) deriving Show

data (NodeAttribute nl, EdgeAttribute el) => CypherNode nl el =
  CypherNode { attrN :: [NAttr] -- ^Attribute: Highest bits of the 32 bit value
             , cols0 :: [CypherComp nl el] -- ^In the beginning empty, grows by evaluating the query
             }

data (NodeAttribute nl, EdgeAttribute el) => CypherEdge nl el =
  CypherEdge { attrE :: [Attr] -- ^Attribute: Highest bits of the 32 bit value
             , edges :: Maybe [NodeEdge] -- ^Filled by evaluation
             , cols1 :: [CypherComp nl el] -- ^In the beginning empty, gathers the components of query
             }

data CypherNodeG nl el =
  CypherNodeG { attrG :: [NAttr] -- ^Attribute: Highest bits of the 32 bit value
              , graphN :: JGraph nl el
              }

data CypherEdgeG nl el =
  CypherEdgeG { attrGE :: [Attr] -- ^Attribute: Highest bits of the 32 bit value
              , edgeRestrG :: Maybe (Word32 -> Maybe (Map (Node,Node) [el]) -> Bool)
              , graphE :: JGraph nl el
              }

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherNode nl el) where 
  show (CypherNode _ _) = "CypherNode"

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherEdge nl el) where 
  show (CypherEdge _ _ _) = "CypherEdge"

data NAttr = AllNodes | Label [Int] | Nodes [Node] deriving Show

data Attr =
     Attr Word32 -- ^Each attribute uses bits that are used only by this attribute
   | Orth Word32 -- ^Attributes can use all bits
   | EFilterBy (Word32 -> Maybe (Map (Node,Node) [Word32]) -> Bool)-- ^Attributes are the result of
                                                      -- filtering all adjacent edges by a function
   | Several Int Int


----------------------------------
-- EDSL trickery from shake

-- edge

-- | A type annotation, equivalent to the first argument, but in variable argument contexts,
--   gives a clue as to what return type is expected (not actually enforced).
type a :-> t = a

edge :: EdgeAttrs as => as :-> CypherEdge nl el
edge = edgeAttrs mempty

newtype EdgeAttr = EdgeAttr [Attr] deriving Monoid

class EdgeAttrs t where
  edgeAttrs :: EdgeAttr -> t

instance (EdgeAttrs r) => EdgeAttrs (EdgeAttr -> r) where
  edgeAttrs xs x = edgeAttrs $ xs `mappend` x

instance (NodeAttribute nl, EdgeAttribute el) => EdgeAttrs (CypherEdge nl el) where
  edgeAttrs (EdgeAttr as) = CypherEdge as Nothing []

-----------
-- node

node :: NodeAttrs as => as :-> CypherNode nl el
node = nodeAttrs mempty

newtype NodeAttr = NodeAttr [NAttr] deriving Monoid

class NodeAttrs t where
  nodeAttrs :: NodeAttr -> t

instance (NodeAttrs r) => NodeAttrs (NodeAttr -> r) where
  nodeAttrs xs x = nodeAttrs $ xs `mappend` x

instance (NodeAttribute nl, EdgeAttribute el) => NodeAttrs (CypherNode nl el) where
  nodeAttrs (NodeAttr as) = CypherNode as []

-----------------------------------------------------
-- Attributes, Labels and WHERE-restrictions

attr :: EdgeAttribute el => el -> EdgeAttr
attr el = EdgeAttr [Attr (fastEdgeAttrBase el)]

orth :: EdgeAttribute el => el -> EdgeAttr
orth el = EdgeAttr [Orth (fastEdgeAttrBase el)]

where_ :: (Word32 -> Maybe (Map (Node,Node) [Word32]) -> Bool) -> EdgeAttr
where_ f = EdgeAttr [EFilterBy f]

several :: Int -> Int -> EdgeAttr
several n0 n1 = EdgeAttr [Several n0 n1]

labels :: (NodeAttribute nl, Enum nl) => [nl] -> NodeAttr
labels nodelabels = NodeAttr [Label (map fromEnum nodelabels)]

-- | Explicitly say which nodes to use, restricted by inbounding edges
nodes32 :: [Word32] -> NodeAttr
nodes32 ns = NodeAttr [Nodes ns]

anyNode :: NodeAttr
anyNode = NodeAttr [AllNodes]


-- | TODO:
--  * (Ortho Vector0) X (Ortho Vector1) X (Attr0 + Attr1 + Attr2)
--
--  * X is Set Product, in this example there would be (2²-1) * 3 = 9 attributes
--
--  * Currently its just [Attr1,Attr2,Attr3]
genAttrs :: [Attr] -> [Word32]
genAttrs as = map getW32 attrs
  where getW32 (Attr e) = e
        attrs = filter isAttr as
        orths = filter isOrtho as
        isOrtho (Orth _) = True
        isOrtho _ = False
        isAttr (Attr _) = True
        isAttr _ = False

----------------------------------------------------------------------------
-- Creating a table
outgoing = 0xf0000000 -- outgoing edge -- TODO check that this bit is unused

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryNE (CypherNode nl el) (CypherEdge nl el) where

  (--|) (CypherNode a0 c0) (CypherEdge a1 e c1)
           | null c0   = CypherEdge a1 e (ce:cn:c0)
           | otherwise = CypherEdge a1 e (   ce:c0)
                                where cn = CN False (CypherNode a0 c0)
                                      ce = CE False (CypherEdge a1 e c1)

  (|--) (CypherEdge a0 e c0) (CypherNode a1 c1)
           | null c0   = CypherNode a1 (cn:ce:c0)
           | otherwise = CypherNode a1 (   cn:c0)
                                where cn = CN False (CypherNode a1 c1)
                                      ce = CE False (CypherEdge a0 e c0)

               -- Assuming the highest bit is not set by someone else
  (<--|) (CypherNode a0 c0) (CypherEdge a1 e c1)
            | null c0   = CypherEdge a1 e (ce:cn:c0)
            | otherwise = CypherEdge a1 e (ce:c0)
                                where ce = CE False (CypherEdge a1 e c1)
                                      cn = CN False (CypherNode a0 c0)

  (|-->) (CypherEdge a0 e c0) (CypherNode a1 c1)
            | null c0   = CypherNode a1 (cn:(CE False directedEdge):c0)
            | otherwise = CypherNode a1 (                        cn:c0)
    where outEdgeAttr = Orth 0 -- (fastEdgeAttrBase (fromJust edgeForward))
          directedEdge = CypherEdge (outEdgeAttr:a0) e c0
          cn = CN False (CypherNode a1 c1)
-- | isNothing edgeForward = error "You use |--> but haven't set edgeForward in EdgeLabel typeclass"

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryN (CypherNode nl el) where
  (-~-) (CypherNode a0 c0) (CypherNode a1 c1)
                 | null c0   = CypherNode a1 (n1:n0:c0)
                 | otherwise = CypherNode a1 (   n1:c0)
                                where n0 = CN False (CypherNode a0 c0)
                                      n1 = CN False (CypherNode a1 c1)

  (-->) (CypherNode a0 c0) (CypherNode a1 c1)
              | null c0   = CypherNode a1 (n1:n0:c0)
              | otherwise = CypherNode a1 (   n1:c0)
                                where n0 = CN False (CypherNode a0 c0)
                                      n1 = CN False (CypherNode a1 c1)
                              -- ((Orthogonal (1, outgoing)):a0)

              -- Assuming the highest bit is not set by someone else
  (<--) (CypherNode a0 c0) (CypherNode a1 c1)
                | null c0   = CypherNode a1 (n1:n0:c0)
                | otherwise = CypherNode a1 (   n1:c0)
                                where n0 = CN False (CypherNode a0 c0)
                                      n1 = CN False (CypherNode a1 c1)

data NE nl = N (LabelNodes nl) | NE [NodeEdge]

instance Show (NE nl) where
  show (N (Ns ns)) = "N " ++ show ns
  show (NE es) = "E [" ++ (intercalate "," (map showHex es)) ++ "]"

class Table nl el a where
  -- | Evaluate the query to a table
  table :: JGraph nl el -> a -> IO [NE nl]

instance (Eq nl, Show nl, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
         Table nl el (CypherNode nl el) where
  -- table :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> CypherNode nl el -> IO [NE nl]
  table graph cypherNode
      | null (cols0 cypherNode) =
          do evalN <- evalNode graph (CypherNode (attrN cypherNode) [])
             evalToTable graph [CN True evalN]
      | otherwise = evalToTable graph (reverse (cols0 cypherNode))

instance (Eq nl, Show nl, NodeAttribute nl, Enum nl, EdgeAttribute el) =>
         Table nl el (CypherEdge nl el) where
  -- table :: (NodeAttribute nl, EdgeAttribute el) => JGraph nl el -> CypherEdge nl el -> IO [NE nl]
  table graph cypherEdge | null (cols1 cypherEdge) = return []
                         | otherwise = evalToTable graph (reverse (cols1 cypherEdge))

----------------------------------------------------------------------------------------------------

evalToTable :: (Eq nl, Show nl, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
               JGraph nl el -> [CypherComp nl el] -> IO [NE nl]
evalToTable graph comps = do res <- runOn graph (Map.fromList (zip [0..] comps))
                             return (map toNE (Map.elems res))
 where
  toNE (CN _ (CypherNode as c)) = N (reduceAttrs as [] [])
  toNE (CE _ (CypherEdge as (Just es) c)) = NE es
  toNE (CE _ (CypherEdge as Nothing c)) = NE []

  reduceAttrs :: (NodeAttribute nl, Enum nl) => [NAttr] -> [Int] -> [Node] -> LabelNodes nl
  reduceAttrs (AllNodes  :as) _ _ = AllNs
  reduceAttrs ((Label ls):as) l n = reduceAttrs as (ls ++ l) n
  reduceAttrs ((Nodes ns):as) l n = reduceAttrs as l (ns ++ n)
  reduceAttrs [] l n | null l    = Ns n
                     | otherwise = Lbl (map toEnum l)


data Compl a = NCompl a | ECompl a deriving Show

fromC (_, NCompl a) = a
fromC (_, ECompl a) = a

-- | Estimating how much work it is to compute these elements
-- TODO use counter of edge-attr
compl (CN eval (CypherNode [AllNodes] c))   = (eval, NCompl 100000) -- Just a high number
compl (CN eval (CypherNode [Label ls] c)) = (eval, NCompl (length ls)) -- Around 1 to 100 labels
compl (CN eval (CypherNode [Nodes n]  c)) = (eval, NCompl 0)-- Nodes contain a low number of (start) nodes
                                     -- 0 because a single label will mostly contain much more nodes
compl (CE eval (CypherEdge a e c))            = (eval, ECompl (length a))


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


evalComp :: (Eq nl, Show nl, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
             JGraph nl el -> (CypherComp nl el) -> IO (CypherComp nl el)
evalComp graph (CN _  c) = do n <- evalNode graph c
                              return (CN True n)

evalNode :: (Eq nl, Show nl, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
             JGraph nl el -> (CypherNode nl el) -> IO (CypherNode nl el)
evalNode graph (CypherNode [Nodes n] c) =
        return (CypherNode [Nodes n] c)

evalNode graph (CypherNode [AllNodes] c) = do
        return (CypherNode [Nodes [0..(nodeCount graph)]] c)

evalNode graph (CypherNode [Label ls] c) = do
        return (CypherNode [Nodes lNodes] c)
  where lNodes = map (toEnum . fromIntegral) (concat (map f spans))
        f :: Enum nl => ((Word32,Word32), nl) -> [Word32]
        f ((start,end), nl) | elem (fromEnum nl) ls = [start..end]
                            | otherwise  = []
        rangeList = toList (ranges graph)
        rangeEnds = map (\x -> x-1) $ (tail (map fst rangeList)) ++ [nodeCount graph]
        spans = zipWith spanGen rangeList rangeEnds
        spanGen (r, nl) e = ((r,e), nl)

extractNodes (CN _ (CypherNode [Nodes ns] c)) = ns

-- | This function repeatedly computes a column in the final table
--   until all columns are evaluated. The columns alternate from left to right between sets of
--   nodes and sets of edges (enforced by the EDSL). The algorithm choses the node-column which is 
--   fastest to compute. Then it choses the edge column left or right (together with the nodes where 
--   it leads) to pick the faster one of the two choices.
runOn :: (Eq nl, Show nl, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
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
       let adjCenter = CN True (CypherNode [Nodes centerNodesAdj] [])
       let restrNodes = CN True (CypherNode [Nodes ns] [])
       let restrEdges = CE True (CypherEdge [] (Just (concat (map nodeEdges es))) [])
       let newCenter = Map.insert minIndex adjCenter comps
       runOn graph (newComponents restrEdges restrNodes newCenter)
 where
  unevaluatedNodes = sum (map unEvalCount elems)
  unEv (CE False e) = True
  unEv _ = False
  elems = Map.elems comps
  getEdges :: Node -> IO (Node, [EdgeAttr32])
  getEdges n | null (attrE lOrR) = do ace <- allChildEdges graph n
                                      return (n, ace)
             | otherwise = (\x -> (n,x)) . concat <$> mapM (adjacentEdgesByAttr graph n) attrs -- (Debug.Trace.trace ("ac " ++ concat (map showHex32 attrs)) n)) attrs
    where attrs | useLeft   = genAttrs (attrE (fromJust leftEdges))
                | otherwise = genAttrs (attrE (fromJust rightEdges))
          lOrR | useLeft   = fromJust leftEdges
               | otherwise = fromJust rightEdges

  nodeEdges :: (Node, [EdgeAttr32]) -> [NodeEdge]
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

