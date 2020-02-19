{-# LANGUAGE OverloadedStrings, FlexibleInstances, MultiParamTypeClasses,
             FunctionalDependencies, UnicodeSyntax, TypeOperators,
             GeneralizedNewtypeDeriving, AllowAmbiguousTypes #-}
{-|
Module      :  Cypher
Description :  Cypher like EDSL to make queries on the graph
Copyright   :  (C) 2017-2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

Neo4j invented Cypher as a sort of SQL for their graph database. But it is much nicer to
have an EDSL in Haskell than a DSL that always misses a command. The ASCII art syntax 
could not be completely taken over, but it is very similar.
-}

module JudyGraph.Cypher(
         -- * Cypher Query EDSL
         QueryN(..), QueryNE(..),
         -- * Cypher Query with Unicode
         (─┤),  (├─),  (<─┤),  (├─>),  (⟞⟝), (⟼),  (⟻),
         -- * Query Components
         CypherComp(..), CypherNode(..), CypherEdge(..), appl,
         -- * Query Evaluation
         GraphCreateReadUpdate(..), NE(..),
         -- * Node specifiers
         node, anyNode, labels, nodes32, NodeAttr(..), NAttr(..),
         -- * Edge specifiers
         edge, attr, orth, where_, several, (…), (***), (...), genAttrs, extractVariants,
         AttrVariants(..),
         -- * Attributes, Labels,...
         EAttr(..), LabelNodes(..),
         -- * Query Evaluation Internals
         evalToTableE, qEvalToTableE, runOnE, evalLtoR, evalNode, GraphDiff(..),
         emptyDiff, switchEvalOff,
         -- * Helpers
         n32n, n32e
       ) where

import           Control.Monad(zipWithM)
import           Data.List(intercalate)
import           Data.List.NonEmpty(toList)
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing, catMaybes)
import           Data.Word(Word32)
import JudyGraph.Enum(GraphClass(..), EnumGraph(..), NodeEdge, 
          Node32(..), Edge32(..), NodeAttribute (..), EdgeAttribute(..),
          empty, allChildNodesFromEdges, adjacentEdgesByAttr,
          allAttrBases, buildWord64, showHex, showHex32, edgeBackward)
import JudyGraph.Table(NAttr(..), EAttr(..), NestedLists(..),
                       applyDeep, zipNAttr, flatten, flatten2, flattenEs)
import System.IO.Unsafe(unsafePerformIO)
-- import Debug.Trace(trace)

----------------------------------------------------------------------
-- Query Classes

-- | Unlabeled edges
class QueryN node where
  -- | An undirected edge
  (~~) :: node -> node -> node

  -- | A directed edge
  (-->) :: node -> node -> node

  -- | A directed edge
  (<--) :: node -> node -> node

-- | Labeled edges
class QueryNE node edge where
  -- | Half of an undirected edge
  (--|)  :: node -> edge -> edge

  -- | Half of an undirected edge
  (|--)  :: edge -> node -> node

  -- | Half of a directed edge
  (<--|) :: node -> edge -> edge

  -- | Half of a directed edge
  (|-->) :: edge -> node -> node

infixl 7  --|
infixl 7 |--
infixl 7 <--|
infixl 7 |-->
infixl 7 ~~
infixl 7 -->
infixl 7 <--

-----------------------------------------
-- More beautiful combinators with unicode
-- Unicode braces ⦃⦄⦗⦘ are not allowed unfortunately

-- | See '--|'
(─┤) :: QueryNE node edge => node -> edge -> edge
(─┤) x y = (--|) x y

-- | See '|--'
(├─) :: QueryNE node edge => edge -> node -> node
(├─) x y = (|--) x y

-- | See '<--|'
(<─┤) :: QueryNE node edge => node -> edge -> edge
(<─┤) x y = (<--|) x y

-- | See '|-->'
(├─>) :: QueryNE node edge => edge -> node -> node
(├─>) x y = (|-->) x y

-- | See '~~'
(⟞⟝) :: QueryN node => node -> node -> node
(⟞⟝) x y = (~~) x y

-- | See '-->'
(⟼) :: QueryN node => node -> node -> node
(⟼) x y = (-->) x y

-- | See '<--'
(⟻) :: QueryN node => node -> node -> node
(⟻) x y = (<--) x y

-- | How often to try an edge, eg 1…3, use keys AltGr and . to get this symbol, or use 1...3
(…) :: Int -> Int -> EdgeAttr
(…) n0 n1 = several n0 n1

-- | How often to try an edge, eg 1…3
(...) :: Int -> Int -> EdgeAttr
(...) n0 n1 = several n0 n1

-- | variable length path
(***) :: EdgeAttr
(***) = several 1 4294967295

infixl 7 ─┤
infixl 7 ├─
infixl 7 <─┤
infixl 7 ├─>
infixl 7  ⟞⟝
infixl 7  ⟼
infixl 7  ⟻

-------------------------------------------------------------------------------------------

data LabelNodes nl = AllNs | Lbl [nl] | Ns [Node32] deriving (Eq, Show)

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
  { attrE :: [EAttr] -- ^Attribute: Highest bits of the 32 bit value
  , edges :: Maybe [(Node32, [Edge32])] -- ^Filled by evaluation
  , cols1 :: [CypherComp nl el] -- ^In the beginning empty, gathers the components of query
  , evaluatedE :: Bool
  }

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherNode nl el) where 
  show(CypherNode att _ _) = "\nN " ++ show att

instance (NodeAttribute nl, EdgeAttribute el) => Show (CypherEdge nl el) where 
  show (CypherEdge _ es _ _) = "\nE " ++ show es

appl :: (NodeAttribute nl, EdgeAttribute el) =>
        ([Node32] -> [Node32]) -> CypherNode nl el -> CypherNode nl el
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
        change (Nodes8 ns) = Nodes8 (map (map (map (map (map (map (map f)))))) ns)
        change (Nodes9 ns) = Nodes9 (map (map (map (map (map (map (map (map f))))))) ns)

-------------------------------------------------------------------------------------------
-- EDSL trickery inspired by shake

---------------------------------------
-- | Bundle several edge specifiers, eg 
--
-- > edge (***) (attr Knows)

-- | A type annotation, equivalent to the first argument, but in variable argument 
--   contexts, gives a clue as to what return type is expected (not actually
--   enforced).
type a :-> t = a

-- | Bundle several edge specifiers, eg 
--
-- > edge (attr Raises) (attr Closes) :: CyE
--
-- Type trickery allows to put as many edge specifiers('EdgeAttr') after 'edge' as you want
edge :: EdgeAttrs as => as :-> CypherEdge nl el
edge = edgeAttrs mempty

newtype EdgeAttr = EdgeAttr [EAttr] deriving (Monoid, Semigroup)

class EdgeAttrs t where
  edgeAttrs :: EdgeAttr -> t

instance (EdgeAttrs r) => EdgeAttrs (EdgeAttr -> r) where
  edgeAttrs xs x = edgeAttrs $ xs `mappend` x

instance (NodeAttribute nl, EdgeAttribute el) => EdgeAttrs (CypherEdge nl el) where
  edgeAttrs (EdgeAttr as) = CypherEdge as Nothing [] False

-----------------------------------------
-- | Bundle several node specifiers, eg 
--
-- > node (labels [ISSUE, PULL_REQUEST]) (nodes32 [0,1])
--
-- Type trickery allows to put as many node specifiers('NodeAttr') after 'node' as you want
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
-- edge specifiers
-- Attributes, Labels and WHERE-restrictions

-- | An Edge Attribute that can be converted to Word32. Several 'attr' mean they are 
--   all followed in a query
attr :: EdgeAttribute el => el -> EdgeAttr
attr el = EdgeAttr [Attr (fastEdgeAttrBase el)]

-- | Short for orthogonal (think of a vector space). The same as
--
-- > (m)<-[:KNOWS|:LOVES]-(n)
--
-- in Neo4j. Instead here we would have to write
--
-- > edge (orth Knows) (orth Loves)
--
-- The 'orth' attribute is treated differently than 'attr'.
-- All combinations of orth attributes are being generated, see 'genAttrs'.
orth :: EdgeAttribute el => el -> EdgeAttr
orth el = EdgeAttr [Orth (fastEdgeAttrBase el)]

-- | Filtering of Attributes with a function
where_ :: (Map Edge32 Bool -> Edge32 -> Bool) -> EdgeAttr
where_ f = EdgeAttr [EFilterBy f]

-- | How often to try an edge, the same as …
several :: Int -> Int -> EdgeAttr
several n0 n1 = EdgeAttr [Several n0 n1]

-- | Generate Word32-edge attributes.
--
-- Let's look at an example where we define:
--
-- X is set product, + set union, and Vector0, Vector1, Attr0, Attr1, Attr2 are Word32
--
--  * (Ortho Vector0) X (Ortho Vector1) X (Attr0 + Attr1 + Attr2)
--
-- In this example there would be (2²-1)*3 = 9 attributes. We would write in our notation:
--
-- > edge (orth Vector0) (orth Vector1) (attr Attr0) (attr Attr1) (attr Attr2)
-- The order does not matter. See 'extractVariants'.
genAttrs :: AttrVariants -> Bool -> [Edge32] -> [Edge32]
genAttrs vs toTheRight allEdges = map addDir gAttrs
  where
    addDir attribute | null (dirs vs) = Edge32 attribute
                     | (toTheRight && leftAr) ||
                       (not toTheRight && not leftAr) = Edge32 (attribute + edgeBackward)
                     | otherwise = Edge32 attribute
    leftAr = lar (head (dirs vs)) where lar DirL = True
                                        lar _ = False
    gAttrs :: [Word32]
    gAttrs | null oAttrs && null aAttrs = map (\(Edge32 e) -> e) allEdges
           | null oAttrs = aAttrs
           | null aAttrs = setProduct oAttrs
           | otherwise = aAttrs ++ (setProduct oAttrs)
                 -- combineAttrs aAttrs (setProduct oAttrs) -- TODO Does this make sense
    aAttrs = map getAW32 (attrs vs) -- added attributes
    oAttrs = map getOW32 (orths vs) -- ortho attributes
    getAW32 (Attr e) = e
    getAW32 _ = 0
    getOW32 (Orth e) = e
    getOW32 _ = 0
--    combineAttrs :: [Word32] -> [Word32] -> [Word32]
--    combineAttrs as os = concat (map (\a -> map (+a) os) as)
    setProduct :: [Word32] -> [Word32]
    setProduct as = tail (sp as [])
    sp :: [Word32] -> [Word32] -> [Word32]
    sp [] es = [sum es]
    sp (a:as) es = (sp as (0:es)) ++
                   (sp as (a:es))


-- | Extract the four different attrs in the given list into four separate lists
extractVariants :: [EAttr] -> AttrVariants
extractVariants vs = variants (AttrVariants [] [] [] [] []) vs
 where
  variants v [] = v
  variants v ((Attr e):rest) = variants (v { attrs = (Attr e) : (attrs v)}) rest
  variants v ((Orth e):rest) = variants (v { orths = (Orth e) : (orths v)}) rest
  variants v ((EFilterBy f):rest) =
      variants (v { eFilter = (EFilterBy f) : (eFilter v)}) rest
  variants v ((Several i0 i1):rest) = variants (v { sev = (Several i0 i1):(sev v)}) rest
  variants v (DirL:rest) = variants (v { dirs = DirL:(dirs v)}) rest
  variants v (DirR:rest) = variants (v { dirs = DirR:(dirs v)}) rest
  variants v (_:rest) = variants v rest

----------------------------------------------------
-- node specifiers

-- | Node specifier that contains all nodes
anyNode :: NodeAttr
anyNode = NodeAttr [AllNodes]

-- | Node specifier that contains all nodes that are in the given label classes
labels :: (NodeAttribute nl, Enum nl) => [nl] -> NodeAttr
labels nodelabels = NodeAttr [Label (map fromEnum nodelabels)]

-- | Node specifier to explicitly say which nodes to use, restricted by inbounding edges
nodes32 :: [Word32] -> NodeAttr
nodes32 ns = NodeAttr [Nodes (map Node32 ns)]


data AttrVariants =
     AttrVariants {
       attrs :: [EAttr],
       orths :: [EAttr],
       eFilter :: [EAttr],
       sev :: [EAttr],
       dirs :: [EAttr]
     }

----------------------------------------------------------------------------
-- Creating a table

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryNE (CypherNode nl el) (CypherEdge nl el) where

  (--|) (CypherNode a0 c0 _) (CypherEdge a1 e c1 b1)
           | null c0   = CypherEdge a1 e (ce:cn:c0) b1
           | otherwise = CypherEdge a1 e (   ce:c0) b1
                           where cn = CN (CypherNode a0 c0 False)
                                 ce = CE (CypherEdge a1 e c1 False)

  (|--) (CypherEdge a0 e c0 _) (CypherNode a1 c1 b1)
           | null c0   = CypherNode a1 (cn:ce:c0) b1
           | otherwise = CypherNode a1 (   cn:c0) b1
                           where cn = CN (CypherNode a1 c1 False)
                                 ce = CE (CypherEdge a0 e c0 False)

  (<--|) (CypherNode a0 c0 _) (CypherEdge a1 e c1 b1)
            | null c0   = CypherEdge a1 e (ce:cn:c0) b1
            | otherwise = CypherEdge a1 e (ce:c0) b1
                           where ce = CE (CypherEdge (DirL:a1) e c1 False)
                                 cn = CN (CypherNode a0 c0 False)

  (|-->) (CypherEdge a0 e c0 _) (CypherNode a1 c1 b1)
            | null c0   = CypherNode a1 (cn:ce:c0) b1
            | otherwise = CypherNode a1 (cn:(replaceCE c0)) b1
                 where cn = CN (CypherNode a1 c1 False)
                       ce = CE (CypherEdge (DirR:a0) e c0 False)
                       replaceCE ((CE (CypherEdge (DirL:a) ed c b)):rest) =
                                 ((CE (CypherEdge       a  ed c b)):rest)
                       replaceCE ((CE (CypherEdge       a  ed c b)):rest) =
                                 ((CE (CypherEdge (DirR:a) ed c b)):rest)
                       replaceCE c = c

instance (NodeAttribute nl, EdgeAttribute el) =>
         QueryN (CypherNode nl el) where
  (~~) (CypherNode a0 c0 _) (CypherNode a1 c1 b1)
          | null c0   = CypherNode a1 (n1:e:n0:[]) b1
          | otherwise = CypherNode a1 (n1:e:c0) b1
                         where n0 = CN (CypherNode a0 c0 False)
                               n1 = CN (CypherNode a1 c1 False)
                               e  = CE (CypherEdge [] Nothing [] False)

  (-->) (CypherNode a0 c0 _) (CypherNode a1 c1 b1)
           | null c0   = CypherNode a1 (n1:e:n0:[]) b1
           | otherwise = CypherNode a1 (n1:e:c0) b1
                          where n0 = CN (CypherNode a0 c0 False)
                                n1 = CN (CypherNode a1 c1 False)
                                e  = CE (CypherEdge [DirR] Nothing [] False)

  (<--) (CypherNode a0 c0 _) (CypherNode a1 c1 b1)
           | null c0   = CypherNode a1 (n1:e:n0:[]) b1
           | otherwise = CypherNode a1 (n1:e:c0) b1
                          where n0 = CN (CypherNode a0 c0 False)
                                n1 = CN (CypherNode a1 c1 False)
                                e  = CE (CypherEdge [DirL] Nothing [] False)

data NE nl = N (LabelNodes nl) | NE [NodeEdge] deriving Eq

instance Show nl => Show (NE nl) where
  show (N ns) = "N " ++ show ns
  show (NE es) = "E [" ++ (intercalate "," (map showHex es)) ++ "]"


class GraphCreateReadUpdate graph nl el a where
  -- | Evaluate the query to a flattened table (every column a list of nodes)
  --   Bool = True : Evaluate from left to right (not calculating a query strategy)
  table :: graph nl el -> Bool -> a -> IO [NE nl]

  -- | The result is evaulated to nested lists and can be reused
  temp :: graph nl el -> Bool -> a -> IO [CypherComp nl el]

  -- | Updates the nodeEdges and returns the changes.
  --   Overwriting an edge means a deleted edge and a new edge
  createMem :: graph nl el -> a -> IO GraphDiff

  -- | Updates the nodeEdges, return diffs, and write changelogs for the db files
--create :: graph nl el -> FilePath -> a -> IO GraphDiff

  -- | The result is a graph
  graphQuery :: graph nl el -> Bool -> a -> IO (graph nl el)

  -- | A graph that already contains nodes with labels is changed in its edges
  graphCreate :: graph nl el -> a -> IO (graph nl el)


data GraphDiff = GraphDiff { diffDelNodes :: DeleteNodes
                           , diffNewNodes :: NewNodes
                           , diffDelEdges :: DeleteEdges
                           , diffNewEdges :: NewEdges
                           }

instance Show GraphDiff where
  show (GraphDiff dn nn de ne) =
       show $ (map (\(Node32 n) -> showHex32 n) dn) ++
              (map (\(Node32 n) -> showHex32 n) nn) ++
              (map (\(nedge, Node32 n) -> showHex nedge ++"|"++ showHex32 n) de) ++
              (map showHex ne)

type DeleteNodes = [Node32]
type NewNodes    = [Node32]
type DeleteEdges = [(NodeEdge,Node32)]
type NewEdges    = [NodeEdge]

instance (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
         GraphCreateReadUpdate EnumGraph nl el (CypherNode nl el) where
  table graph quickStrat cypherNode
      | null (cols0 cypherNode) =
                 do (CypherNode a c _) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
                    if quickStrat then qEvalToTableE graph [CN (CypherNode a c True)]
                                  else  evalToTableE graph [CN (CypherNode a c True)]
      | quickStrat = qEvalToTableE graph (reverse (cols0 cypherNode))
      | otherwise  =  evalToTableE graph (reverse (cols0 cypherNode))

  temp graph _ cypherNode -- TODO temp graph quickstrat cypherNode
    | null (cols0 cypherNode) =
      do (CypherNode a c _) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
         return [CN (CypherNode a c False)]
    | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                       (runOnE graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  createMem graph cypherNode
      | null (cols0 cypherNode) = return emptyDiff -- TODO
      | otherwise = fmap snd (runOnE graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols0 cypherNode)

  graphQuery graph quickStrat cypherNode
      | null (cols0 cypherNode) =
          do (CypherNode a c _) <- evalNode graph (CypherNode (attrN cypherNode) [] False)
             if quickStrat then qEvalToGraph graph [CN (CypherNode a c True)]
                           else  evalToGraph graph [CN (CypherNode a c True)]
      | quickStrat = qEvalToGraph graph (reverse (cols0 cypherNode))
      | otherwise  =  evalToGraph graph (reverse (cols0 cypherNode))

  graphCreate gr _ = return gr


instance (Eq nl, Show nl, Show el, NodeAttribute nl, Enum nl, EdgeAttribute el) =>
         GraphCreateReadUpdate EnumGraph nl el (CypherEdge nl el) where
  table graph quickStrat cypherEdge
      | null (cols1 cypherEdge) = return []
      | quickStrat = qEvalToTableE graph (reverse (cols1 cypherEdge))
      | otherwise  =  evalToTableE graph (reverse (cols1 cypherEdge))

  temp graph _ cypherEdge -- TODO temp graph quickStrat cypherEdge
      | null (cols1 cypherEdge) = return []
      | otherwise = fmap (map switchEvalOff . Map.elems . fst)
                         (runOnE graph False emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  createMem graph cypherEdge
      | null (cols1 cypherEdge) = return emptyDiff
      | otherwise = fmap snd (runOnE graph True emptyDiff (Map.fromList (zip [0..] comps)))
    where comps = reverse (cols1 cypherEdge)

  graphQuery graph quickStrat cypherEdge
      | null (cols1 cypherEdge) = empty (rangesE graph)
      | quickStrat = qEvalToGraph graph (reverse (cols1 cypherEdge))
      | otherwise  =  evalToGraph graph (reverse (cols1 cypherEdge))

  graphCreate gr _ = return gr


switchEvalOff :: (NodeAttribute nl, EdgeAttribute el) => CypherComp nl el -> CypherComp nl el
switchEvalOff (CN (CypherNode a c _))   = CN (CypherNode a c False)
switchEvalOff (CE (CypherEdge a e c _)) = CE (CypherEdge a e c False)

emptyDiff :: GraphDiff
emptyDiff = GraphDiff [] [] [] []

------------------------------------------------------------------------------------------
-- |
evalToTableE :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                EnumGraph nl el -> [CypherComp nl el] -> IO [NE nl]
evalToTableE graph comps =
  do (res,_) <- runOnE graph False emptyDiff (Map.fromList (zip [0..] comps))
--     return (map toNE (Map.elems (trace ("eval" ++ show res) res)))
     return (map toNE (Map.elems res))

qEvalToTableE :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                EnumGraph nl el -> [CypherComp nl el] -> IO [NE nl]
qEvalToTableE graph comps =
  do res <- evalLtoR graph False emptyDiff comps
--     return (map toNE (fst (trace ("qEval" ++ show res) res)))
     return (map toNE (fst res))

toNE :: (NodeAttribute nl, EdgeAttribute el, Enum nl) => CypherComp nl el -> NE nl
toNE (CN (CypherNode as _ _)) = N (reduceAttrs as [] [])
toNE (CE (CypherEdge _ (Just es) _ _)) = NE (concat (map nodeEdges es))
toNE (CE (CypherEdge _ Nothing _ _)) = NE []


reduceAttrs :: (NodeAttribute nl, Enum nl) => [NAttr] -> [Int] -> [Node32] -> LabelNodes nl
reduceAttrs (AllNodes  :_) _ _ = AllNs
reduceAttrs ((Label ls):as) l n = reduceAttrs as (ls ++ l) n
reduceAttrs ((Nodes ns):as) l n = reduceAttrs as l (ns ++ n)
reduceAttrs ((Nodes2 ns):as) l n = reduceAttrs as l (flatten (Nodes2 ns) ++ n)
reduceAttrs ((Nodes3 ns):as) l n = reduceAttrs as l (flatten (Nodes3 ns) ++ n)
reduceAttrs ((Nodes4 ns):as) l n = reduceAttrs as l (flatten (Nodes4 ns) ++ n)
reduceAttrs ((Nodes5 ns):as) l n = reduceAttrs as l (flatten (Nodes5 ns) ++ n)
reduceAttrs ((Nodes6 ns):as) l n = reduceAttrs as l (flatten (Nodes6 ns) ++ n)
reduceAttrs ((Nodes7 ns):as) l n = reduceAttrs as l (flatten (Nodes7 ns) ++ n)
reduceAttrs ((Nodes8 ns):as) l n = reduceAttrs as l (flatten (Nodes8 ns) ++ n)
reduceAttrs ((Nodes9 ns):as) l n = reduceAttrs as l (flatten (Nodes9 ns) ++ n)
reduceAttrs [] l n | null l    = Ns n
                   | otherwise = Lbl (map toEnum l)

data Compl a = NCompl a | ECompl a deriving Show

fromC :: (Bool, Compl a) -> a
fromC (_, NCompl a) = a
fromC (_, ECompl a) = a

-- | Estimating how much work it is to compute these elements
-- TODO use counter of edge-attr
compl :: (NodeAttribute nl, EdgeAttribute el) => CypherComp nl el -> (Bool, Compl Int)
compl (CN (CypherNode [AllNodes] _ eval)) =
  (eval, NCompl 100000) -- Just a high number
compl (CN (CypherNode [Label ls] _ eval)) =
  (eval, NCompl (length ls)) -- Around 1 to 100 labels
compl (CN (CypherNode _ _ eval)) =
  (eval, NCompl 0) -- Nodes contain a low number of (start) nodes
                   -- 0 because a single label will most likely contain much more nodes
compl (CE (CypherEdge a _ _ eval)) = (eval, ECompl (length a))


minI :: Int -> Int -> Int -> [(Bool, Compl Int)] -> Int
minI _ i _ [] = i -- trace ("i " ++ show i) i
minI n i min_ ((True, _) : (True, x):cs) = -- trace ("eTrue " ++ show i)
              (minI (n+1) i min_ ((True, x):cs)) -- skip evaluated nodes

minI n i min_ ((True, NCompl c):(False, _):cs)
  | c < min_   = -- trace ("c < min"++ show i)
                 (minI (n+1) n c   cs)
  | otherwise = -- trace ("other " ++ show (i,c,min) )
                (minI (n+1) i min_ cs)

minI n i min_ ((True, _):cs) = -- trace ("eTrue " ++ show i)
                               (minI (n+1) i min_ cs) -- skip evaluated nodes

minI n i min_ ((False, NCompl c):cs)
  | c < min_   = -- trace ("c < min"++ show i)
                 (minI (n+1) n c   cs)
  | otherwise = -- trace ("other " ++ show (i,c,min) )
                (minI (n+1) i min_ cs)

minI n i min_ ((False, ECompl _):cs) = -- trace ("e " ++ show i)
                                       (minI (n+1) i min_ cs) -- skip edges

unEvalCount :: (NodeAttribute nl, EdgeAttribute el) => CypherComp nl el -> Int
unEvalCount (CN (CypherNode _ _ evaluated))   = if evaluated then 0 else 1
unEvalCount (CE (CypherEdge _ _ _ evaluated)) = if evaluated then 0 else 1


evalComp :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el,
             GraphClass graph nl el) =>
             graph nl el -> (CypherComp nl el) -> IO (CypherComp nl el)
evalComp graph (CN (CypherNode a c b)) =
  do (CypherNode a1 c1 _) <- evalNode graph (CypherNode a c b)
     return (CN (CypherNode a1 c1 True))
evalComp _ c = return c

-- | Convert an abstract node specifier to a concrete node specifier
--   (containing a list of nodes)
evalNode :: (GraphClass graph nl el, Show nl, Show el, NodeAttribute nl, EdgeAttribute el, Enum nl) =>
            graph nl el -> (CypherNode nl el) -> IO (CypherNode nl el)
evalNode graph (CypherNode [AllNodes] c b) =
  do return (CypherNode [ Nodes (map Node32 [0..((nodeCount graph)-1)]) ] c b)

evalNode graph (CypherNode [Label ls] c b) = do
        return (CypherNode [Nodes lNodes] c b)
  where lNodes = -- trace ("sp" ++ show rangeList ++"\n"++ show (ls, spans, map f spans)) $
                 map (Node32 . toEnum . fromIntegral) (concat (map f spans))
        f :: Enum nl => ((Word32,Word32), nl) -> [Word32]
        f ((start,end), nl) | elem (fromEnum nl) ls = [start..end]
                            | otherwise  = []
        rangeList = toList (ranges graph)
        rangeEnds = map (\x -> x-1) $ (tail (map (fst . fst) rangeList)) ++ [nodeCount graph]
        spans = zipWith spanGen rangeList rangeEnds
        spanGen ((r,_), (nl, _)) e = ((r,e), nl)

evalNode _ (CypherNode ns c b) =
        return (CypherNode ns c b)


extractNodes :: (NodeAttribute nl, EdgeAttribute el) => CypherComp nl el -> [NAttr]
extractNodes (CN (CypherNode ns _ _)) = ns
extractNodes _ = []


-- | A lot of queries are developed with an intuition that they are evaluated
--  from left to right. By doing this we can avoid the time to construct
--  a query strategy, like with 'runOnE'
evalLtoR :: (Eq nl, Show nl, Show el, Enum nl,
          NodeAttribute nl, EdgeAttribute el) =>
         EnumGraph nl el -> Bool -> GraphDiff ->
         [CypherComp nl el] -> IO ([CypherComp nl el], GraphDiff)
evalLtoR graph create (GraphDiff dns newns des newdEs) (n0:e0:_:comps)
  | not (startsWithNode n0) =
    error "a query has to start with a node in evalLtoR in Cypher.hs"
  | otherwise = do
     evalCenter <- if unEv n0 then evalComp graph n0 else return n0
     let startNs = extractNodes evalCenter :: [NAttr]
     (_, nEs, ns, (delEs,newNEs), count) <- walkPaths graph create startNs [] True r ([],[]) 1
     let adjCenter = CN (CypherNode startNs [] True)
     let ne = if count == 1 then Just nEs -- (concat (map nodeEdges nEs))
                            else Nothing -- only show edges when there is length 1 path
     let restrEdges = CE (CypherEdge [] ne [] True)
     let restrNodes = -- trace (show (cNAdj, nEs, ns, (delEs,newNEs), count))
                                         (CN (CypherNode ns [] True))
     (cs, diff) <-
       if noMoreNodesFound ns
       then return ([], GraphDiff dns newns des newdEs)
       else evalLtoR graph create (GraphDiff dns newns (des ++ delEs) (newdEs ++ newNEs))
                                  (restrNodes:comps)
     return (if null cs
             then -- trace ("null cs"++ show (cNAdj, nEs, ns, (delEs,newNEs), count, startNs, r, graph))
                  []
             else adjCenter : restrEdges : cs, diff)
 where
  r = unCE e0
  noMoreNodesFound n | null (map flatten n) = True
                     | otherwise = -- trace ("(n0,e0,n1)"++ show (n0,e0,n1))
                                   (null (head (map flatten n)))

evalLtoR graph _ diff [n0]
  | not (startsWithNode n0) =
    error "a query has to start with a node in evalLtoRGraph in Cypher.hs"
  | unEv n0 = do
     evalCenter <- evalComp graph n0 -- (trace (show n0) n0)
     let startNs = extractNodes evalCenter :: [NAttr]
     let restrNodes = CN (CypherNode startNs [] True)
     return ([restrNodes], diff)
  | otherwise = return ([n0], diff)

evalLtoR _ _ diff _ = return ([], diff)


startsWithNode :: (NodeAttribute nl, EdgeAttribute el) => CypherComp nl el -> Bool
startsWithNode (CN (CypherNode _ _ _)) = True
startsWithNode _ = False

unEv :: (NodeAttribute nl, EdgeAttribute el) => CypherComp nl el -> Bool
unEv (CE (CypherEdge _ _ _ False)) = True
unEv (CN (CypherNode _ _ False)) = True
unEv _ = False

unCE :: CypherComp nl el -> CypherEdge nl el
unCE (CE x) = x
unCE _ = error "unCE"

-- | This function repeatedly computes a column in the final table
--   until all columns are evaluated. The columns alternate from left to right between 
--   sets of nodes and sets of edges (enforced by the EDSL). The algorithm choses the 
--   node-column which is fastest to compute. Then it choses the edge column left or
--   right (together with the nodes where it leads) to pick the faster one of the
--   two choices. If 'several' is used the columns are skipped
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
    (cNAdj, nEs, newNs, (delEs,newNEs), _) <-
             walkPaths graph create startNs [] True lOrR ([],[]) 1 -- TODO True
    let adjCenter  = CN (CypherNode [toNAttr cNAdj] [] True)
    let restrNodes = CN (CypherNode (map delNulls newNs) [] True)
    let restrEdges = CE (CypherEdge [] (Just (filter (not . null . snd) nEs)) [] True) -- (concat (map nodeEdges nEs))) [] True)
    let newCenter = -- Debug.Trace.trace ("ns: "++ show (cNAdj,newNs)) $
                    Map.insert minInd (if useLeft then restrNodes else adjCenter) comps
    runOnE graph create (GraphDiff dns newns delEs newNEs)
         (newComponents restrEdges (if useLeft then adjCenter else restrNodes) newCenter)
 where
  unevaluatedNs = sum (map unEvalCount elems)
  elems = Map.elems comps

  computeComplexity = Map.map compl comps
  minInd = -- trace ("\nelems " ++ show comps ++"\n"++
           --                    (show (Map.elems computeComplexity)))
                             (minI 0 0 1000000 (Map.elems computeComplexity))

  costLeft  = Map.lookup (minInd - 2) computeComplexity
  costRight = Map.lookup (minInd + 2) computeComplexity

  leftNodes  =          Map.lookup (minInd - 2) comps
  leftEdges  = unCE <$> Map.lookup (minInd - 1) comps
  center     = -- trace ("minIndex " ++ show minInd) $
               Map.lookup minInd comps
  rightEdges = unCE <$> Map.lookup (minInd + 1) comps
  rightNodes =          Map.lookup (minInd + 2) comps

  lOrR | useLeft   = fromJust leftEdges
       | otherwise = fromJust rightEdges

  newComponents :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                   (CypherComp nl el)
                -> (CypherComp nl el)
                -> Map Int (CypherComp nl el) -> Map Int (CypherComp nl el)
  newComponents es ns newCenter
    | useLeft && isJust leftNodes = -- trace ("useLeft1"++ show (ns, es, newCenter)) $
                                    Map.insert (minInd-2) ns
                                   (Map.insert (minInd-1) es newCenter)
    | useLeft && isNothing leftNodes = -- trace ("useLeft2"++ show (ns, es, newCenter)) $
                                       Map.insert (minInd-1) es newCenter
    | isJust rightNodes =           -- trace ("useRight"++ show (ns, es, newCenter)) $
                                    Map.insert (minInd+2) ns
                                   (Map.insert (minInd+1) es newCenter)
    | otherwise =                   -- trace ("useRight1"++ show (ns, es, newCenter)) $
                                    Map.insert (minInd+1) es newCenter

  useLeft | (isJust costLeft && isNothing costRight) ||
            (isJust costLeft && isJust costRight &&
                (unEv (fromJust leftNodes)) &&
                (fromC (fromJust costLeft) < fromC (fromJust costRight))) ||
            (isJust leftEdges && isNothing rightEdges) = True
          | otherwise = False

  delNulls (Nodes2 ns) = Nodes2 (filter (not . null) ns)
  delNulls ns = ns

nodeEdges :: (Node32, [Edge32]) -> [NodeEdge]
nodeEdges (Node32 n, es) = map (\(Edge32 e) -> buildWord64 n e) es

-- | reduce the typing work (see examples)
n32n :: (Word32,nl) -> (Node32,nl)
n32n (n,nl) = (Node32 n, nl)

-- | reduce the typing work (see examples)
n32e :: ((Word32, Word32), d0) -> ((Node32, Node32), Maybe a0, Maybe a1, d0, Bool)
n32e ((from,to),ls) = ((Node32 from, Node32 to), Nothing, Nothing, ls, True)


-- | If an edge has to be walked repeatedly, eg with "edge (1…3)"
walkPaths :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
             EnumGraph nl el -> Bool -> [NAttr] -> [(Node32, [Edge32])] -> Bool ->
             CypherEdge nl el -> ([(NodeEdge, Node32)], [NodeEdge]) -> Int ->
         IO ([Node32], [(Node32, [Edge32])], [NAttr], ([(NodeEdge, Node32)], [NodeEdge]), Int)
walkPaths graph create startNs oldEs toTheRight lOrR (startDelEs,startNewNEs) count = do
    adjacentEdges <- mapM (applyDeep getEdges) startNs
    newNodes <- zipWithM (zipNAttr graph (allChildNodesFromEdges graph)) startNs adjacentEdges
    let flatEs = map flattenEs adjacentEdges :: [[[Edge32]]]
    let flatNs = map flatten2 newNodes :: [[[Node32]]]
    (delEs,newNEs) <- if create
                      then overlaps graph (concat $ zipWith3 addNs flatSNs flatEs flatNs)
                      else return ([],[])
    let ns = -- trace ("nn "++ show (adjacentEdges, newNodes, (repeatStart, repeatEnd, count)))
             newNodes -- TODO: If already evaluated at this place,
                    -- choose smaller list and probably reduce node lists in this direction
    let nEdges = concat $ zipWith zip flatSNs flatEs :: [(Node32, [Edge32])]
    let centerNodesAdj = map fst (filter (\(_,es) -> not (null es)) nEdges)
--    trace ("walkPaths"++ show (nEdges) ) $
    stopRecursion centerNodesAdj ns nEdges (delEs,newNEs)
--        (Debug.Trace.trace ("walk"++ show (startNs, adjacentEdges, newNodes)) centerNodesAdj) ns nEdges (delEs,newNEs)
 where
  flatSNs = map flatten startNs :: [[Node32]]

  stopRecursion centerNodesAdj ns nEdges (delEs,newNEs)
      | enull && (count == 1 || count < repeatStart) = -- stop recursion, failed query
        return (-- trace "nc0 "
                ([], [], [], ([],[]), count))
      | (not enull) && count >= repeatEnd = -- succesful query result, we could go further but we stop here
        return (-- trace ("nc1 "++ show (enull,count,repeatStart,repeatEnd,centerNodesAdj, oldEs, startNs))
                (centerNodesAdj, nEdges, ns, (delEs,newNEs), count))
      | enull = -- did not reach repeatEnd, but cannot go any further => succesful query result
        return (-- trace ("nc2 "++ show (enull,count,repeatStart,repeatEnd,centerNodesAdj, oldEs, startNs))
                (centerNodesAdj, oldEs, startNs, (delEs,newNEs), count))
      | otherwise = -- trace ("nc3 "++ show (enull,count,ns, nEdges)) $ -- query not finished yet
                    walkPaths graph create ns nEdges toTheRight lOrR
                              (startDelEs ++ delEs, startNewNEs ++ newNEs) (count+1)
    where enull | null (map snd nEdges) = True
                | otherwise = and (map null (map snd nEdges))

  getEdges :: Node32 -> IO [Edge32]
  getEdges n | null (eFilter variants) = -- trace ("\nattrs0 " ++ (show (n,attributes,allBases))) $
               concat <$> mapM (adjacentEdgesByAttr graph n) attributes
             | otherwise = -- trace ("\nattrs1 " ++ (show (n,attributes,allBases))) $
                           filterBy (head (eFilter variants)) <$>
                           concat <$> mapM (adjacentEdgesByAttr graph n) attributes
    where
      attributes = genAttrs variants toTheRight allBases
      allBases = unsafePerformIO $ allAttrBases graph n
      filterBy (EFilterBy f) = filter (f Map.empty)
      filterBy _ = id

  variants = extractVariants (attrE lOrR)

  getSeveral :: AttrVariants -> [EAttr]
  getSeveral (AttrVariants _ _ _ s _) = s

  (Several repeatStart repeatEnd) | null vs = Several 1 1
                                  | otherwise = head vs
    where vs = getSeveral variants

  addNs :: [Node32] -> [[Edge32]] -> [[Node32]] -> [(Node32, [(Edge32, Node32)])]
  addNs n0 es ns = zipWith3 addN n0 es ns
  addN :: Node32 -> [Edge32] -> [Node32] -> (Node32, [(Edge32, Node32)])
  addN n0 es ns = (n0, zip es ns)


-- | Add nodeEdges to the graph and remember the changes so that the graph on the ssd can
--   be adjusted with edges to delete and add.
overlaps :: (NodeAttribute nl,EdgeAttribute el,Show nl,Show el,GraphClass graph nl el) =>
            graph nl el -> [(Node32, [(Edge32,Node32)])]
            -> IO ([(NodeEdge, Node32)], [NodeEdge])
overlaps jgraph nes =
  do o <- mapM overlap nes
     return (concat (map fst o), -- nodeEdges that have to be deleted (on disk)
             concat (map snd o)) -- nodeEdges that are new (have to be added on disk)
 where
--  j = judyGraph jgraph
  overlap :: (Node32, [(Edge32, Node32)]) -> IO ([(NodeEdge, Node32)], [NodeEdge])
  overlap (Node32 n0, es) = do
      ls <- mapM test es
      return (catMaybes (map fst ls), map snd ls)
    where
      test :: (Edge32, Node32) -> IO (Maybe (NodeEdge, Node32), NodeEdge)
      test (Edge32 e, n1) = do
        (_,(isNew,(n2,_))) <- insertNodeEdgeAttr True jgraph
                              ((Node32 n0, n1), Nothing, Nothing, Edge32 e, Edge32 e)
-- insertNodeEdgeAttr useMJ overwrite jgraph ((n0, n1), nl0, nl1, edgeAttr, edgeAttrBase)
        let newEdge = buildWord64 n0 e
        let delEdge = if isNew then Just (newEdge, n2) else Nothing
        return (delEdge, newEdge)


-------------------------------------------------------------------------------
-- Creating a graph

evalToGraph :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                EnumGraph nl el -> [CypherComp nl el] -> IO (EnumGraph nl el)
evalToGraph graph _ = -- TODO
  do return graph


qEvalToGraph :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
                EnumGraph nl el -> [CypherComp nl el] -> IO (EnumGraph nl el)
qEvalToGraph graph comps = fmap fst (evalLtoRGraph graph False emptyDiff comps)


evalLtoRGraph :: (Eq nl, Show nl, Show el, Enum nl, NodeAttribute nl, EdgeAttribute el) =>
         EnumGraph nl el -> Bool -> GraphDiff -> [CypherComp nl el] -> IO (EnumGraph nl el, GraphDiff)
evalLtoRGraph graph create (GraphDiff dns newns des newdEs) (n0:e0:_:comps)
  | not (startsWithNode n0) =
    error "a query has to start with a node in evalLtoRGraph in Cypher.hs"
  | otherwise = do
     evalCenter <- if unEv n0 then evalComp graph n0 else return n0
     let startNs = extractNodes evalCenter :: [NAttr]
     (_, _, ns, (delEs,newNEs), _) <- walkPaths graph create startNs [] True r ([],[]) 1
--     let adjCenter = CN (CypherNode startNs [] True)

--     let ne = if count == 1 then Just (concat (map nodeEdges nEs))
--                            else Nothing -- only show edges when there is length 1 path

--     let restrEdges = CE (CypherEdge [] ne [] True)
     let restrNodes = -- trace (show (cNAdj, nEs, ns, (delEs,newNEs), count))
                                         (CN (CypherNode ns [] True))
     (gr, diff) <-
       if noMoreNodesFound ns
       then return (graph, GraphDiff dns newns des newdEs)
       else evalLtoRGraph graph create (GraphDiff dns newns (des ++ delEs) (newdEs ++ newNEs))
                                  (restrNodes:comps)
     isEmpty <- isNull gr
     return (if isEmpty then graph else graph, diff) -- adjCenter : restrEdges : gr
 where
  r = unCE e0
  noMoreNodesFound n | null (map flatten n) = True
                     | otherwise = -- trace ("(n0,e0,n1)"++ show (n0,e0,n1))
                                   (null (head (map flatten n)))

evalLtoRGraph graph _ diff [n0]
  | not (startsWithNode n0) =
    error "a query has to start with a node in evalLtoRGraph in Cypher.hs"
  | unEv n0 = do
--     evalCenter <- evalComp graph n0 -- (trace (show n0) n0)
--     let startNs = extractNodes evalCenter :: [NAttr]
--     let restrNodes = CN (CypherNode startNs [] True)
     return (graph, diff)
  | otherwise = return (graph, diff) -- n0

evalLtoRGraph graph _ diff _ = return (graph, diff)

