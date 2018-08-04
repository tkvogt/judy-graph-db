{-# LANGUAGE OverloadedStrings, FlexibleInstances, MultiParamTypeClasses,
             FunctionalDependencies, UnicodeSyntax, TypeOperators,
             GeneralizedNewtypeDeriving, AllowAmbiguousTypes #-}
-- {-# TypeSynonymInstances, ScopedTypeVariables #-}
--, InstanceSigs, ScopedTypeVariables #-}
{-|
Module      :  Cypher
Description :  Cypher like EDSL to make queries on the graph
Copyright   :  (C) 2017-2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

Neo4j invented Cypher as a sort of SQL for their graph database. But it is much nicer to
have an EDSL in Haskell than a DSL that always misses a command. The ASCII art syntax could 
not be completely taken over, but it is very similar.
-}

module JudyGraph.Cypher(
         -- * Cypher Query
         QueryN(..), QueryNE(..),
         -- * Cypher Query with Unicode
         (─┤),  (├─),  (<─┤),  (├─>),  (⟞⟝), (⟼),  (⟻),
         -- * Query Components
         CypherComp(..), CypherNode(..), CypherEdge(..),
         -- * Query Evaluation
         GraphCreateReadUpdate(..), NE(..),
         -- * Setting of Attributes, Labels,...
         NAttr(..), Attr(..), edge, node, attr, where_, several, (…),
         LabelNodes(..),
         -- * Unevaluated node/edge markers, 
         anyNode, labels, nodes32,
         -- * changing markers
         appl,
         -- * query evaluation
         GraphDiff(..), runOnE, emptyDiff, switchEvalOff, evalToTableE, evalNode
       ) where

import           Control.Monad(zipWithM)
import qualified Data.Judy as J
import           Data.List(sortBy, intercalate)
import           Data.List.NonEmpty(toList)
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing, catMaybes)
import           Data.Semigroup
import qualified Data.Set as Set
import           Data.Set(Set)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import JudyGraph.Enum(GraphClass(..), JGraph(..), EnumGraph(..), Judy(..), Node, NodeEdge,
          NodeAttribute (..), EdgeAttribute(..), Bits(..), EdgeAttr32, empty, hasNodeAttr,
          allChildNodesFromEdges, allChildEdges, adjacentEdgesByAttr, buildWord64,
          lookupJudyNodes, showHex, showHex32)
import System.IO.Unsafe(unsafePerformIO)
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
  (~~) :: node -> node -> node

  -- | A directed edge
  (-->) :: node -> node -> node

  -- | A directed edge
  (<--) :: node -> node -> node

infixl 7  --|
infixl 7 |--
infixl 7 <--|
infixl 7 |-->
infixl 7 ~~
infixl 7 -->
infixl 7 <--

-----------------------------------------
-- More beatiful combinators with unicode

(─┤) x y = (--|) x y -- Unicode braces ⟬⟭⦃⦄⦗⦘ are not allowed
(├─) x y = (|--) x y
(<─┤) x y = (<--|) x y
(├─>) x y = (|-->) x y

(⟞⟝) x y = (~~) x y
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

-- | After evluating the query the result is a list of CypherComps, 
--   which have to be evaluated again
data CypherComp nl el = CN (CypherNode nl el)
                      | CE (CypherEdge nl el) -- deriving Show
instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherComp nl el) where
  show (CN cn) = show cn
  show (CE ce) = show ce

data (NodeAttribute nl, EdgeAttribute el) => CypherNode nl el =
  CypherNode
    { attrN :: [NAttr] -- ^Attribute: Highest bits of the 32 bit value
    , cols0 :: [CypherComp nl el] -- ^In the beginning empty, grows by evaluating the query
    , evaluatedN :: Bool
    }

data (NodeAttribute nl, EdgeAttribute el) => CypherEdge nl el =
  CypherEdge
    { attrE :: [Attr] -- ^Attribute: Highest bits of the 32 bit value
    , edges :: Maybe [NodeEdge] -- ^Filled by evaluation
    , cols1 :: [CypherComp nl el] -- ^In the beginning empty, gathers the components of query
    , evaluatedE :: Bool
    }

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherNode nl el) where 
  show (CypherNode attr _ _) = "\nN " ++ show attr

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherEdge nl el) where 
  show (CypherEdge attr _ _ _) = "\nE " ++ show attr

data NAttr = AllNodes
           | Label  [Int]
           | Nodes  [Node]
           | Nodes2 [[Node]]
           | Nodes3 [[[Node]]]
           | Nodes4 [[[[Node]]]]
           | Nodes5 [[[[[Node]]]]]
           | Nodes6 [[[[[[Node]]]]]]
           | Nodes7 [[[[[[[Node]]]]]]] -- That should be enough
              deriving Show


-- Keep structure and apply a IO-function to the lowest nesting of nodes
applyDeep :: (Node -> IO [Node]) -> NAttr -> IO NAttr
applyDeep f (Nodes ns)  = fmap Nodes2 $ sequence (map f ns)
applyDeep f (Nodes2 ns) = fmap Nodes3 $ sequence (map (sequence . (map f)) ns)
applyDeep f (Nodes3 ns) = fmap Nodes4 $
  sequence (map (sequence . (map (sequence . (map f)))) ns)
applyDeep f (Nodes4 ns) = fmap Nodes5 $
  sequence (map (sequence . (map (sequence . (map (sequence . (map f)))))) ns)
applyDeep f (Nodes5 ns) = fmap Nodes6 $
  sequence (map (sequence .
           (map (sequence .
           (map (sequence . (map (sequence . (map f)))))))) ns)
applyDeep f (Nodes6 ns) = fmap Nodes7 $
  sequence (map (sequence .
           (map (sequence .
           (map (sequence .
           (map (sequence . (map (sequence . (map f)))))))))) ns)


-- Keep structure and apply a IO-function to the lowest nesting of two lists
zipNAttr :: (NodeAttribute nl, EdgeAttribute el) =>
            graph nl el -> (Node -> [EdgeAttr32] -> IO [Node])
            -> NAttr -- Node -- first 32 bit of several node-edges
            -> NAttr -- [EdgeAttr32] -- several second 32 bit
            -> IO NAttr -- [Node]) -- looked up nodes
zipNAttr graph f (Nodes  ns) (Nodes2 es) = fmap Nodes2 $ zipWithM f ns es
zipNAttr graph f (Nodes2 ns) (Nodes3 es) = fmap Nodes3 $ zipWithM (zipWithM f) ns es
zipNAttr graph f (Nodes3 ns) (Nodes4 es) =
  fmap Nodes4 $ zipWithM (zipWithM (zipWithM f)) ns es
zipNAttr graph f (Nodes4 ns) (Nodes5 es) =
  fmap Nodes5 $ zipWithM (zipWithM (zipWithM (zipWithM f))) ns es
zipNAttr graph f (Nodes5 ns) (Nodes6 es) =
  fmap Nodes6 $ zipWithM (zipWithM (zipWithM (zipWithM (zipWithM f)))) ns es
zipNAttr graph f (Nodes6 ns) (Nodes7 es) =
  fmap Nodes7 $ zipWithM (zipWithM (zipWithM (zipWithM (zipWithM (zipWithM f))))) ns es


class NestedLists a where
  toNAttr :: a -> NAttr

instance NestedLists [Node] where toNAttr ns = Nodes ns
instance NestedLists [[Node]] where toNAttr ns = Nodes2 ns
instance NestedLists [[[Node]]] where toNAttr ns = Nodes3 ns
instance NestedLists [[[[Node]]]] where toNAttr ns = Nodes4 ns
instance NestedLists [[[[[Node]]]]] where toNAttr ns = Nodes5 ns
instance NestedLists [[[[[[Node]]]]]] where toNAttr ns = Nodes6 ns
instance NestedLists [[[[[[[Node]]]]]]] where toNAttr ns = Nodes7 ns

data Attr =
     Attr Word32 -- ^Each attribute uses bits that are used only by this attribute
   | Orth Word32 -- ^Attributes can use all bits
   | EFilterBy (Map (Node,Node) [Word32]
                -> Word32
                -> Bool) -- ^Attributes are the result of
                         -- filtering all adjacent edges by a function
   | Several Int Int

instance Show Attr where
  show (Attr w32) = "Attr" ++ show w32
  show (Orth w32) = "Orth" ++ show w32
  show (EFilterBy f) = "EFilterBy"
  show (Several i0 i1) = show i0 ++ "…" ++ show i1

appl f n = CypherNode (map change (attrN n)) (cols0 n) True
  where change AllNodes = AllNodes
        change (Label ls) = Label ls
        change (Nodes ns) = Nodes (f ns)
        change (Nodes2 ns) = Nodes2 (map f ns)
        change (Nodes3 ns) = Nodes3 (map (map f) ns)
        change (Nodes4 ns) = Nodes4 (map (map (map f)) ns)
        change (Nodes5 ns) = Nodes5 (map (map (map (map f))) ns)
        change (Nodes6 ns) = Nodes6 (map (map (map (map (map f)))) ns)
        change (Nodes7 ns) = Nodes7 (map (map (map (map (map (map f))))) ns)

----------------------------------
-- EDSL trickery inspired by shake

-- edge

-- | A type annotation, equivalent to the first argument, but in variable argument 
--   contexts, gives a clue as to what return type is expected (not actually
--   enforced).
type a :-> t = a

edge :: EdgeAttrs as => as :-> CypherEdge nl el
edge = edgeAttrs mempty

newtype EdgeAttr = EdgeAttr [Attr] deriving (Monoid, Semigroup)

class EdgeAttrs t where
  edgeAttrs :: EdgeAttr -> t

instance (EdgeAttrs r) => EdgeAttrs (EdgeAttr -> r) where
  edgeAttrs xs x = edgeAttrs $ xs `mappend` x

instance (NodeAttribute nl, EdgeAttribute el) => EdgeAttrs (CypherEdge nl el) where
  edgeAttrs (EdgeAttr as) = CypherEdge as Nothing [] False

-----------
-- node

node :: NodeAttrs as => as :-> CypherNode nl el
node = nodeAttrs mempty

newtype NodeAttr = NodeAttr [NAttr] deriving (Monoid, Semigroup)

class NodeAttrs t where
  nodeAttrs :: NodeAttr -> t

instance (NodeAttrs r) => NodeAttrs (NodeAttr -> r) where
  nodeAttrs xs x = nodeAttrs $ xs `mappend` x

instance (NodeAttribute nl, EdgeAttribute el) => NodeAttrs (CypherNode nl el) where
  nodeAttrs (NodeAttr as) = CypherNode as [] False

-----------------------------------------------------
-- Attributes, Labels and WHERE-restrictions

attr :: EdgeAttribute el => el -> EdgeAttr
attr el = EdgeAttr [Attr (fastEdgeAttrBase el)]

orth :: EdgeAttribute el => el -> EdgeAttr
orth el = EdgeAttr [Orth (fastEdgeAttrBase el)]

where_ :: (Map (Node,Node) [Word32] -> Word32 -> Bool) -> EdgeAttr
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


data AttrVariants =
     AttrVariants {
       attrs :: [Attr],
       orths :: [Attr],
       eFilter :: [Attr],
       sev :: [Attr]
     }

-- | Generate Word32-edge attributes.
-- Example:
--  * (Ortho Vector0) X (Ortho Vector1) X (Attr0 + Attr1 + Attr2)
--
--  * X is Set Product, in this example there would be (2²-1) * 3 = 9 attributes
genAttrs :: Map (Node,Node) [Word32] -> AttrVariants -> [Word32]
genAttrs m vs | null (eFilter vs) = genAttrs
              | otherwise = filterBy (head (eFilter vs)) genAttrs
  where
    filterBy (EFilterBy f) = filter (f m)
    genAttrs | null oAttrs = aAttrs
             | null aAttrs = setProduct oAttrs
             | otherwise = combineAttrs aAttrs (setProduct oAttrs)
    aAttrs = map getAW32 (attrs vs) -- added attributes
    oAttrs = map getOW32 (orths vs) -- ortho attributes
    getAW32 (Attr e) = e
    getOW32 (Orth e) = e
    combineAttrs :: [Word32] -> [Word32] -> [Word32]
    combineAttrs as os = concat (map (\a -> map (+a) os) as)
    setProduct :: [Word32] -> [Word32]
    setProduct as = tail (sp as [])
    sp :: [Word32] -> [Word32] -> [Word32]
    sp [] es = [sum es]
    sp (a:as) es = (sp as (0:es)) ++
                   (sp as (a:es))


extractVariants :: [Attr] -> AttrVariants
extractVariants vs = variants (AttrVariants [] [] [] []) vs
  where
    variants v [] = v
    variants v ((Attr e):rest) = variants (v { attrs = (Attr e) : (attrs v)}) rest
    variants v ((Orth e):rest) = variants (v { orths = (Orth e) : (orths v)}) rest
    variants v ((EFilterBy f):rest) = variants (v { eFilter = (EFilterBy f) : (eFilter v)}) rest
    variants v ((Several i0 i1):rest) = variants (v { sev = (Several i0 i1) : (sev v)}) rest


----------------------------------------------------------------------------
-- Creating a table
outgoing = 0xf0000000 -- outgoing edge -- TODO check that this bit is unused

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryNE (CypherNode nl el) (CypherEdge nl el) where

  (--|) (CypherNode a0 c0 b0) (CypherEdge a1 e c1 b1)
           | null c0   = CypherEdge a1 e (ce:cn:c0) b1
           | otherwise = CypherEdge a1 e (   ce:c0) b1
                                where cn = CN (CypherNode a0 c0 False)
                                      ce = CE (CypherEdge a1 e c1 False)

  (|--) (CypherEdge a0 e c0 b0) (CypherNode a1 c1 b1)
           | null c0   = CypherNode a1 (cn:ce:c0) b1
           | otherwise = CypherNode a1 (   cn:c0) b1
                                where cn = CN (CypherNode a1 c1 False)
                                      ce = CE (CypherEdge a0 e c0 False)

               -- Assuming the highest bit is not set by someone else
  (<--|) (CypherNode a0 c0 b0) (CypherEdge a1 e c1 b1)
            | null c0   = CypherEdge a1 e (ce:cn:c0) b1
            | otherwise = CypherEdge a1 e (ce:c0) b1
                                where ce = CE (CypherEdge a1 e c1 False)
                                      cn = CN (CypherNode a0 c0 False)

  (|-->) (CypherEdge a0 e c0 b0) (CypherNode a1 c1 b1)
            | null c0   = CypherNode a1 (cn:(CE directedEdge):c0) b1
            | otherwise = CypherNode a1 (                  cn:c0) b1
    where outEdgeAttr = Orth 0 -- (fastEdgeAttrBase (fromJust edgeForward))
          directedEdge = CypherEdge (outEdgeAttr:a0) e c0 False
          cn = CN (CypherNode a1 c1 False)
-- | isNothing edgeForward =
--   error "You use |--> but haven't set edgeForward in EdgeLabel typeclass"

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryN (CypherNode nl el) where
  (~~) (CypherNode a0 c0 b0) (CypherNode a1 c1 b1)
                 | null c0   = CypherNode a1 (n1:e:n0:[]) b1
                 | otherwise = CypherNode a1 (n1:e:c0) b1
                                where n0 = CN (CypherNode a0 c0 False)
                                      n1 = CN (CypherNode a1 c1 False)
                                      e  = CE (CypherEdge [] Nothing [] False)

  (-->) (CypherNode a0 c0 b0) (CypherNode a1 c1 b1)
              | null c0   = CypherNode a1 (n1:e:n0:[]) b1
              | otherwise = CypherNode a1 (n1:e:c0) b1
                                where n0 = CN (CypherNode a0 c0 False)
                                      n1 = CN (CypherNode a1 c1 False)
                                      e  = CE (CypherEdge [] Nothing [] False)
                              -- ((Orthogonal (1, outgoing)):a0)

              -- Assuming the highest bit is not set by someone else
  (<--) (CypherNode a0 c0 b0) (CypherNode a1 c1 b1)
                | null c0   = CypherNode a1 (n1:e:n0:[]) b1
                | otherwise = CypherNode a1 (n1:e:c0) b1
                                where n0 = CN (CypherNode a0 c0 False)
                                      n1 = CN (CypherNode a1 c1 False)
                                      e  = CE (CypherEdge [] Nothing [] False)

data NE nl = N (LabelNodes nl) | NE [NodeEdge]

instance Show nl => Show (NE nl) where
  show (N ns) = "N " ++ show ns
  show (NE es) = "E [" ++ (intercalate "," (map showHex es)) ++ "]"


class GraphCreateReadUpdate graph nl el a where
  -- | Evaluate the query to a flattened table (every column a list of nodes)
  table :: graph nl el -> a -> IO [NE nl]

  -- | The result is evaulated to nested lists and can be reused
  temp :: graph nl el -> a -> IO [CypherComp nl el]

  -- | Updates the nodeEdges and returns the changes.
  --   Overwriting an edge means a deleted edge and a new edge
  createMem :: graph nl el -> a -> IO GraphDiff

  -- | Updates the nodeEdges, return diffs, and write changelogs for the db files
--create :: EnumGraph nl el -> FilePath -> a
--          -> IO ((DeleteNodes, NewNodes), (DeleteEdges, NewEdges))

  -- | The result is a graph
  graphQuery :: graph nl el -> a -> IO (graph nl el)

  -- | A graph that already contains nodes with labels is changed in its edges
  graphCreate :: graph nl el -> a -> IO (graph nl el)


data GraphDiff = GraphDiff { diffDelNodes :: DeleteNodes
                           , diffNewNodes :: NewNodes
                           , diffDelEdges :: DeleteEdges
                           , diffNewEdges :: NewEdges
                           }

instance Show GraphDiff where
  show (GraphDiff dn nn de ne) =
       show $ (map showHex32 dn) ++ (map showHex32 nn) ++
              (map (\(ne,n) -> showHex ne ++"|"++ showHex32 n) de) ++ (map showHex ne)

type DeleteNodes = [Node]
type NewNodes    = [Node]
type DeleteEdges = [(NodeEdge,Node)]
type NewEdges    = [NodeEdge]

instance (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
         GraphCreateReadUpdate EnumGraph nl el (CypherNode nl el) where
  table graph cypherNode
      | null (cols0 cypherNode) =
          do (CypherNode a c b) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             evalToTableE graph [CN (CypherNode a c True)]
      | otherwise = evalToTableE graph (reverse (cols0 cypherNode))

  temp graph cypherNode
      | null (cols0 cypherNode) =
          do (CypherNode a c evalN) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             return [CN (CypherNode a c False)]
      | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                         (runOnE graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  createMem graph cypherNode
      | null (cols0 cypherNode) = return (GraphDiff [] [] [] []) -- TODO
      | otherwise = fmap snd (runOnE graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  graphQuery graph cypherNode | null (cols0 cypherNode) =
          do (CypherNode a c b) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             evalToGraph graph [CN (CypherNode a c True)]
                              | otherwise = evalToGraph graph (reverse (cols0 cypherNode))

  graphCreate gr cypherNode = return gr


instance (Eq nl, Show nl, Show el, NodeAttribute nl, Enum nl, EdgeAttribute el) =>
         GraphCreateReadUpdate EnumGraph nl el (CypherEdge nl el) where
  table graph cypherEdge | null (cols1 cypherEdge) = return []
                         | otherwise = evalToTableE graph (reverse (cols1 cypherEdge))

  temp graph cypherEdge
      | null (cols1 cypherEdge) = return []
      | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                         (runOnE graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  createMem graph cypherEdge
      | null (cols1 cypherEdge) = return (GraphDiff [] [] [] [])
      | otherwise = fmap snd (runOnE graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  graphQuery graph cypherEdge | null (cols1 cypherEdge) = empty (rangesE graph)
                              | otherwise = evalToGraph graph (reverse (cols1 cypherEdge))

  graphCreate gr cypherEdge = return gr


switchEvalOff (CN (CypherNode a c b))   = CN (CypherNode a c False)
switchEvalOff (CE (CypherEdge a e c b)) = CE (CypherEdge a e c False)

emptyDiff = GraphDiff [] [] [] []

-------------------------------------------------------------------------------------------

evalToTableE :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                EnumGraph nl el -> [CypherComp nl el] -> IO [NE nl]
evalToTableE graph comps =
  do (res,_) <- runOnE graph False emptyDiff (Map.fromList (zip [0..] comps))
     return (map toNE (Map.elems res))
 where
  toNE (CN (CypherNode as c _)) = N (reduceAttrs as [] [])
  toNE (CE (CypherEdge as (Just es) c _)) = NE es
  toNE (CE (CypherEdge as Nothing c _)) = NE []


reduceAttrs :: (NodeAttribute nl, Enum nl) => [NAttr] -> [Int] -> [Node] -> LabelNodes nl
reduceAttrs (AllNodes  :as) _ _ = AllNs
reduceAttrs ((Label ls):as) l n = reduceAttrs as (ls ++ l) n
reduceAttrs ((Nodes ns):as) l n = reduceAttrs as l (ns ++ n)
reduceAttrs ((Nodes2 ns):as) l n = reduceAttrs as l (flatten (Nodes2 ns) ++ n)
reduceAttrs ((Nodes3 ns):as) l n = reduceAttrs as l (flatten (Nodes3 ns) ++ n)
reduceAttrs ((Nodes4 ns):as) l n = reduceAttrs as l (flatten (Nodes4 ns) ++ n)
reduceAttrs ((Nodes5 ns):as) l n = reduceAttrs as l (flatten (Nodes5 ns) ++ n)
reduceAttrs ((Nodes6 ns):as) l n = reduceAttrs as l (flatten (Nodes6 ns) ++ n)
reduceAttrs [] l n | null l    = Ns n
                   | otherwise = Lbl (map toEnum l)

flatten (Nodes ns) = ns
flatten (Nodes2 ns) = concat ns
flatten (Nodes3 ns) = concat (concat ns)
flatten (Nodes4 ns) = concat (concat (concat ns))
flatten (Nodes5 ns) = concat (concat (concat (concat ns)))
flatten (Nodes6 ns) = concat (concat (concat (concat (concat ns))))

flatten2 (Nodes ns) = [ns]
flatten2 (Nodes2 ns) = ns
flatten2 (Nodes3 ns) = concat ns
flatten2 (Nodes4 ns) = concat (concat ns)
flatten2 (Nodes5 ns) = concat (concat (concat ns))
flatten2 (Nodes6 ns) = concat (concat (concat (concat ns)))

data Compl a = NCompl a | ECompl a deriving Show

fromC (_, NCompl a) = a
fromC (_, ECompl a) = a

-- | Estimating how much work it is to compute these elements
-- TODO use counter of edge-attr
compl (CN (CypherNode [AllNodes] c eval)) =
  (eval, NCompl 100000) -- Just a high number
compl (CN (CypherNode [Label ls] c eval)) =
  (eval, NCompl (length ls)) -- Around 1 to 100 labels
compl (CN (CypherNode [ns] c eval)) =
  (eval, NCompl 0) -- Nodes contain a low number of (start) nodes
                   -- 0 because a single label will mostly contain much more nodes
compl (CE (CypherEdge a e c eval)) = (eval, ECompl (length a))


minI n i min [] = i -- Debug.Trace.trace ("i " ++ show i) i
minI n i min ((True, _):(True, x):cs) = -- Debug.Trace.trace ("eTrue " ++ show i)
                                        (minI (n+1) i min ((True, x):cs)) -- skip evaluated nodes

minI n i min ((True, NCompl c):(False, e):cs)
  | c < min   = -- Debug.Trace.trace ("c < min"++ show i)
                (minI (n+1) n c   cs)
  | otherwise = -- Debug.Trace.trace ("other " ++ show (i,c,min) )
                (minI (n+1) i min cs)

minI n i min ((True, x):cs) = -- Debug.Trace.trace ("eTrue " ++ show i)
                              (minI (n+1) i min cs) -- skip evaluated nodes

minI n i min ((False, NCompl c):cs)
  | c < min   = -- Debug.Trace.trace ("c < min"++ show i)
                (minI (n+1) n c   cs)
  | otherwise = -- Debug.Trace.trace ("other " ++ show (i,c,min) )
                (minI (n+1) i min cs)

minI n i min ((False, ECompl c):cs) = -- Debug.Trace.trace ("e " ++ show i)
                                      (minI (n+1) i min cs) -- skip edges


unEvalCount (CN (CypherNode _ _ evaluated)) = if evaluated then 0 else 1
unEvalCount (CE (CypherEdge _ _ _ evaluated)) = if evaluated then 0 else 1


evalComp :: (Eq nl, Show nl, Enum nl, NodeAttribute nl, EdgeAttribute el,
             GraphClass graph nl el) =>
             graph nl el -> (CypherComp nl el) -> IO (CypherComp nl el)
evalComp graph (CN (CypherNode a c b)) =
  do (CypherNode a1 c1 b1) <- evalNode graph (CypherNode a c b)
     return (CN (CypherNode a1 c1 True))

evalNode :: (GraphClass graph nl el, NodeAttribute nl, EdgeAttribute el, Enum nl) =>
            graph nl el -> (CypherNode nl el) -> IO (CypherNode nl el)
evalNode graph (CypherNode [AllNodes] c b) =
  do return (CypherNode [Nodes [0..(nodeCount graph)]] c b)

evalNode graph (CypherNode [Label ls] c b) = do
        return (CypherNode [Nodes lNodes] c b)
  where lNodes = --Debug.Trace.trace ("sp" ++ show spans) $
                 map (toEnum . fromIntegral) (concat (map f spans))
        f :: Enum nl => ((Word32,Word32), nl) -> [Word32]
        f ((start,end), nl) | elem (fromEnum nl) ls = [start..end]
                            | otherwise  = []
        rangeList = toList (ranges graph)
        rangeEnds = map (\x -> x-1) $ (tail (map fst rangeList)) ++ [nodeCount graph]
        spans = zipWith spanGen rangeList rangeEnds
        spanGen (r, nl) e = ((r,e), nl)

evalNode graph (CypherNode ns c b) =
        return (CypherNode ns c b)


extractNodes (CN (CypherNode ns c _)) = ns


-- | This function repeatedly computes a column in the final table
--   until all columns are evaluated. The columns alternate from left to right between 
--   sets of nodes and sets of edges (enforced by the EDSL). The algorithm choses the 
--   node-column which is fastest to compute. Then it choses the edge column left or
--   right (together with the nodes where it leads) to pick the faster one of the two choices.
runOnE :: (Eq nl, Show nl, Show el, Enum nl,
          NodeAttribute nl, EdgeAttribute el) =>
         EnumGraph nl el -> Bool -> GraphDiff ->
         Map Int (CypherComp nl el) -> IO (Map Int (CypherComp nl el), GraphDiff)
runOnE graph create (GraphDiff dns newns des newEs) comps
  | unevaluatedNs == 0 = return (comps, GraphDiff dns newns des newEs)
  | unevaluatedNs == 1 =
    if unEv (head elems)
    then return (comps, GraphDiff dns newns des newEs) -- TODO edges to anynode
    else return (comps, GraphDiff dns newns des newEs) -- unEv (last elems)
  | otherwise =
 do evalCenter <- evalComp graph (fromJust center)
    let startNs = extractNodes evalCenter :: [NAttr]
    adjacentEdges <- mapM (applyDeep getEdges) startNs

    let es = -- Debug.Trace.trace (show startNs ++ " ee " ++ show adjacentEdges) $
             adjacentEdges :: [NAttr] -- TODO apply WHERE restriction
    newNodes <- zipWithM (zipNAttr graph (allChildNodesFromEdges graph)) startNs es
    let flatSNs = map flatten startNs :: [[Node]]
    let flatEs = map flatten2 es :: [[[EdgeAttr32]]]
    let flatNs = map flatten2 newNodes :: [[[Node]]]
    (delEs,newNEs) <- if create
                      then overlaps graph (concat $ zipWith3 addNs flatSNs flatEs flatNs)
                      else return ([],[])
    let ns =
        -- Debug.Trace.trace ("nn "++ show newNodes ++" "++ show unevaluatedNs ++" "++ show es)
             newNodes -- TODO: If already evaluated at this place,
                      -- choose smaller list and probably reduce node lists in this direction
    let nEdges = concat $ zipWith zip flatSNs flatEs :: [(Node, [EdgeAttr32])]
    let centerNodesAdj = map fst (filter (\(n,es) -> not (null es)) nEdges)
    let adjCenter  = CN (CypherNode [toNAttr centerNodesAdj] [] True)
    let restrNodes = CN (CypherNode ns [] True)
    let restrEdges = CE (CypherEdge [] (Just (concat (map nodeEdges nEdges))) [] True)
    let newCenter = Map.insert minInd (if useLeft then restrNodes else adjCenter) comps
    runOnE graph create (GraphDiff dns newns delEs newNEs)
         (newComponents restrEdges (if useLeft then adjCenter else restrNodes) newCenter)
 where
  addNs :: [Node] -> [[EdgeAttr32]] -> [[Node]] -> [(Node, [(EdgeAttr32, Node)])]
  addNs n0 es ns = zipWith3 addN n0 es ns
  addN :: Node -> [EdgeAttr32] -> [Node] -> (Node, [(EdgeAttr32, Node)])
  addN n0 es ns = (n0, zip es ns)
  unevaluatedNs = sum (map unEvalCount elems)
  unEv (CE (CypherEdge a e c False)) = True
  unEv (CN (CypherNode a c False)) = True
  unEv _ = False
  elems = Map.elems comps
  getEdges :: Node -> IO [EdgeAttr32]
  getEdges n | null (attrE lOrR) =
               Debug.Trace.trace ("attrE" ++ show (unsafePerformIO $ allChildEdges graph n)) $
               allChildEdges graph n
             | otherwise = Debug.Trace.trace ("other" ++ show attrs) $
                           concat <$> mapM (adjacentEdgesByAttr graph n) attrs
--   (Debug.Trace.trace ("ac " ++ concat (map showHex32 attrs)) n)) attrs
    where attrs | useLeft   = genAttrs Map.empty (extractVariants (attrE (fromJust leftEdges)))
                | otherwise = genAttrs Map.empty (extractVariants (attrE (fromJust rightEdges)))
          lOrR | useLeft   = fromJust leftEdges
               | otherwise = fromJust rightEdges

  computeComplexity = Map.map compl comps
  minInd = Debug.Trace.trace ("\nelems " ++ show comps ++"\n"++
                               (show (Map.elems computeComplexity)))
                             (minI 0 0 1000000 (Map.elems computeComplexity))

  costLeft  = Map.lookup (minInd - 2) computeComplexity
  costRight = Map.lookup (minInd + 2) computeComplexity

  leftNodes  =          Map.lookup (minInd - 2) comps
  leftEdges  = unCE <$> Map.lookup (minInd - 1) comps
  center     = Debug.Trace.trace ("minIndex " ++ show minInd) $
               Map.lookup minInd comps
  rightEdges = unCE <$> Map.lookup (minInd + 1) comps
  rightNodes =          Map.lookup (minInd + 2) comps

  unCN (CN x) = x
  unCE (CE x) = x

  newComponents :: (CypherComp nl el)
                -> (CypherComp nl el)
                -> Map Int (CypherComp nl el) -> Map Int (CypherComp nl el)
  newComponents es ns newCenter
    | useLeft && isJust leftNodes = Debug.Trace.trace "useLeft1"
                                    Map.insert (minInd-2) ns
                                   (Map.insert (minInd-1) es newCenter)
    | useLeft && isNothing leftNodes = Debug.Trace.trace "useLeft2"
                                       Map.insert (minInd-1) es newCenter
    | isJust rightNodes =           Debug.Trace.trace "useRight"
                                    Map.insert (minInd+2) ns
                                   (Map.insert (minInd+1) es newCenter)
    | otherwise =                   Debug.Trace.trace "useRight1"
                                    Map.insert (minInd+1) es newCenter

  useLeft | (isJust costLeft && isNothing costRight) ||
            (isJust costLeft && isJust costRight &&
                (unEv (fromJust leftNodes)) &&
                (fromC (fromJust costLeft) < fromC (fromJust costRight))) ||
            (isJust leftEdges && isNothing rightEdges) = True
          | otherwise = False


nodeEdges :: (Node, [EdgeAttr32]) -> [NodeEdge]
nodeEdges (n,as) = map (buildWord64 n) as


-- | Add nodeEdges to the graph and remember the changes so that the graph on the ssd can
--   be adjusted with edges to delete and add.
overlaps :: (NodeAttribute nl, EdgeAttribute el, Show nl, Show el, GraphClass graph nl el) =>
            graph nl el -> [(Node, [(EdgeAttr32,Node)])]
            -> IO ([(NodeEdge, Node)], [NodeEdge])
overlaps jgraph nes =
  do o <- mapM overlap nes
     return (concat (map fst o), -- nodeEdges that have to be deleted (on disk)
             concat (map snd o)) -- nodeEdges that are new (have to be added on disk)
  where
    j = judyGraph jgraph
    overlap :: (Node, [(EdgeAttr32, Node)]) -> IO ([(NodeEdge, Node)], [NodeEdge])
    overlap (n0,es) = do ls <- mapM test es
                         return (catMaybes (map fst ls), map snd ls)
      where
        test :: (EdgeAttr32, Node) -> IO (Maybe (NodeEdge, Node), NodeEdge)
        test (e,n1) = do
          (_, (isNew, n2)) <- insertNodeEdgeAttr True jgraph
                                                 ((n0, n1), Nothing, Nothing, e, e)
-- insertNodeEdgeAttr useMJ overwrite jgraph ((n0, n1), nl0, nl1, edgeAttr, edgeAttrBase)
          let newEdge = buildWord64 n0 e
          let delEdge = if isNew then Just (newEdge, n2) else Nothing
          return (delEdge, newEdge)


-------------------------------------------------------------------------------
-- Creating a graph TODO

evalToGraph :: (Eq nl, Show nl, Show el, Enum nl,
                NodeAttribute nl, EdgeAttribute el, GraphClass graph nl el) =>
                graph nl el -> [CypherComp nl el] -> IO (graph nl el)
evalToGraph graph comps =
  do return graph

