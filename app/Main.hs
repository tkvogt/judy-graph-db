{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Data.Bits(shift)
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)

import JudyGraph
import qualified JudyGraph as J
import qualified JudyGraph.Cypher as Cy

main :: IO ()
main = do
  jgraph <- J.fromListE False nodes dirEdges [] ranges

  -- Which of the issues that simon has raised reference other issues?
  query <- temp jgraph (simon --| raises |-- issue --| references |-- issue)
  putStrLn ("query result: " ++ show query)

  main2
 where
  simon  = node (nodes32 [0]) :: CyN
  raises = edge (attr Raises) :: CyE -- (attr Closes) :: CyE
  issue  = node (labels [ISSUE]) :: CyN

  -- on my keyboard … is <alt Gr> + <.>
  references = edge (attr References) (where_ restrict) (1…3) :: CyE
  restrict w32 edgeMap = True

  nodes :: [(J.Node, NodeLabel)]
  nodes = [(0, PROGRAMMER), (1, PROGRAMMER),
           (2, ORGANISATION),
           (3, ISSUE), (4, ISSUE), (5, ISSUE), (6, ISSUE),
           (7, PULL_REQUEST)]

  -- ranges are normally generated automatically from graph files,
  -- but here we do it by hand
  ranges = NonEmpty.fromList [(0, PROGRAMMER), (2, ORGANISATION),
                              (3, ISSUE), (7, PULL_REQUEST)]

  dirEdges :: [(J.Edge, [EdgeLabel])]
  dirEdges = [((0,3), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((0,4), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((0,5), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((0,6), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((3,5), [References]), -- ISSUE References ISSUE
              ((4,3), [References]), -- ISSUE References ISSUE
              ((4,6), [References]), -- ISSUE References ISSUE
              ((5,4), [Closes]),     -- PULL_REQUEST Closes ISSUE
              ((0,3), [Closes]),     -- PROGRAMMER Closes ISSUE
              ((1,7), [Accepts]),    -- PROGRAMMER Accepts PULL_REQUEST
              ((0,2), [BelongtsTO])] -- PROGRAMMER BelongtsTO ORGANISATION

-------------------------------------------------------------------------------------------

type CyN = CypherNode NodeLabel EdgeLabel
type CyE = CypherEdge NodeLabel EdgeLabel

data NodeLabel = PROGRAMMER | ORGANISATION | ISSUE | PULL_REQUEST deriving (Eq, Show, Enum)

-- | Can be complex (like a record). Figure out which attributes are important for 
--   filtering edges
data EdgeLabel = Raises | Accepts | Closes | References | BelongtsTO | EdgeForward
  deriving Show

instance NodeAttribute NodeLabel where
    fastNodeAttr _ = (0, 0) -- we don't use node attrs

instance EdgeAttribute EdgeLabel where
    -- What a programmer can do
  fastEdgeAttr Raises      = (8,0x1000001) -- take the 8 highest bits
  fastEdgeAttr Accepts     = (8,0x2000001) -- 1 higher than fastEdgeAttrBase because of counter
  fastEdgeAttr Closes      = (8,0x3000001)
  fastEdgeAttr BelongtsTO  = (8,0x4000001)

  -- What an issue can do
  fastEdgeAttr References  = (8,0x5000001)

  -- A property all edges can have
  fastEdgeAttr EdgeForward = (8,0x80000000) -- the highest bit


  fastEdgeAttrBase Raises     = 0x1000000
  fastEdgeAttrBase Accepts    = 0x2000000
  fastEdgeAttrBase Closes     = 0x3000000
  fastEdgeAttrBase BelongtsTO = 0x4000000

  fastEdgeAttrBase References = 0x5000000
  fastEdgeAttrBase EdgeForward = 0x80000000

  edgeForward = Just EdgeForward
  addCsvLine _ graph _ = return graph

---------------------------------------------------------------------------------------------

{-
The second example shows how to do post processing of a graph. A query on the graph is used
to find adjacent nodes, then we sort them and insert them again with "createMem" or "create".


Example: Calculate dependency boundaries
-------------------------------------

@
  build-depends:       aeson
                     , base >= 4.7 && < 5
@

When the upper and lower version boundary of a library dependency needs to be found, 
we need to know in which version of a library a function exists. We see a version
of a library as a set of functions with name, type and namespace (other properties 
left away for simplicity in this  example).
The correct boundaries come from an intersection of all sets, with equal functions in the
intersections. We can broaden this equality later to find name changes of functions, bug
fixes, and other things. The intersection of sets is nothing one should do again and again
for every imported function. Therefore we insert edges into the graph that connect all
functions that are equal from the lowest version to the highest where it appears.
If we import several functions we only have to follow these edges and find the lowest(highest) 
common version of all functions. Although following edges with judy arrays is also O(log n),
it is faster than looking up strings of types and function-names in Data.Map. We also later 
want to do structure comparison of functions only once. Thats why this is a handy post
processing step to speed up queries.

To avoid a O(n²) brute force comparison of every node from set A to every node from Set B
(intersection), we sort all functions in a package after type and name: O(n log n), and
insert them again in the graph. This way of reconnecting the nodes also allows to find
renaming of functions, namespaces when the type hasn't changed. Of course the type alone
is not enough to find equality of two functions. In the real implementation we also compare
the internal structure of the functions, but the filtering by the same type reduces the
search space a lot.

-}

main2 :: IO ()
main2 = do
  jgraph <- J.fromListE False nodes dirEdges [] ranges
--  [CN _ p, _, CN _ v, _, CN _ f] <- temp jgraph (packages --> packagesVer --> function)
  query <- temp jgraph (packages --> packagesVer --> function)
--  createMem jgraph (p --> v --> appl sort f)
--  create jgraph dbPath (p --> v --> appl sort f)
  putStrLn ("\nquery result: " ++ show query)
  putStrLn "Done"
 where
  packages    = node (labels [PACKAGE ""]) :: CyN2
  packagesVer = node (labels [PACKAGEVER ""]) :: CyN2
  function    = node (labels [FUNCTION F]) :: CyN2
  sort ns = ns

  nodes :: [(J.Node, NodeLabel2)]
  nodes = [(0, PACKAGE "test"),
           (1, PACKAGEVER "test-0.1"), (2, PACKAGEVER "test-0.2"), (3, PACKAGEVER "test-0.3"),
           (4, FUNCTION (Func "Int -> Bool" "odd"  "MyPrelude")),
           (5, FUNCTION (Func "Int -> Bool" "even" "MyPrelude")),
           (6, FUNCTION (Func "Int -> Int"  "bell" "Test.Speculate.Utils"))]

  -- ranges are normally generated automatically from graph files, but here we do it by hand
  ranges = NonEmpty.fromList [(0, PACKAGE ""), (1, PACKAGEVER ""), (4, FUNCTION F)]

  dirEdges :: [(J.Edge, [EdgeLabel2])]
  dirEdges =
    [((0,1), [PartOf]), ((0,2), [PartOf]), ((0,3), [PartOf]),
     ((1,4), [PartOf]), ((2,4), [PartOf]), ((3,4), [PartOf]), -- "odd" is part of all three vers
                        ((2,5), [PartOf]), ((3,5), [PartOf]), -- "even" exists since test-0.2
                        ((2,6), [PartOf]) -- "bell" is only used in test-0.2
    ]

------------------------------------------------------------------------------------------------

type CyN2 = CypherNode NodeLabel2 EdgeLabel2
type CyE2 = CypherEdge NodeLabel2 EdgeLabel2

data NodeLabel2 = PACKAGE Text | PACKAGEVER Text | FUNCTION Func deriving (Eq, Show)

instance Enum NodeLabel2 where
  toEnum 0 = PACKAGE ""
  toEnum 1 = PACKAGEVER ""
  toEnum 2 = FUNCTION F
  fromEnum (PACKAGE _) = 0
  fromEnum (PACKAGEVER _) = 1
  fromEnum (FUNCTION _) = 2

data Func =
     Func { functionType :: !Text
       , functionName :: !Text
       , nameSpace :: !Text
       }
     | F deriving (Eq ,Ord, Show)

-- | Can be complex (like a record).
--   Figure out which attributes are important for filtering edges
data EdgeLabel2 = PartOf deriving Show

instance NodeAttribute NodeLabel2 where
    fastNodeAttr _ = (0, 0) -- we don't use node attrs

instance EdgeAttribute EdgeLabel2 where
    -- What a programmer can do
    fastEdgeAttr PartOf = (8,0x1000001) -- take the 8 highest bits
    fastEdgeAttrBase PartOf = 0x1000000
    edgeForward = Nothing
    addCsvLine _ graph _ = return graph

---------------------------------------------
{-
(==) (ty0, n0, struct0, mod0)
     (ty1, n1, struct1, mod1) =

-- Sort all functions in a package after types and names: O(n log n)
  -- Sort after names, make edges between equal functions (same name, type)
    -- module of the function changed
    ((n0 == n1) && (ty0 == ty1) && (mod0 /= mod1))

    -- implementation of function changed
    ((n0 == n1) && (ty0 == ty1) && (mod0 == mod1))

  -- analyse the unconnected nodes if implementation is the same (costly)
    -- name of the function changed
    ((n0 /= n1) && (ty0 == ty1) && (mod0 == mod1))

    -- name and module of the function changed
    ((n0 /= n1) && (ty0 == ty1) && (mod0 /= mod1))

  -- Only name is preserved, type has changed (and therefore also implementation). 
  -- What is more likely: Modification or new function? Has to be tested on real data
    ((n0 == n1) && (ty0 /= ty1) && (mod0 == mod1)

    -- If there are still unconnected nodes, then they are either new to Set B, or have been deleted from A
-}

