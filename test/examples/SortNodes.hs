{-# LANGUAGE OverloadedStrings, BinaryLiterals #-}
{-|
Module      :  SortNodes
Copyright   :  (C) 2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

This example shows how to do post processing of a graph. A query on the graph is used to find 
adjacent nodes, then we sort them and insert them again with "createMem" or "create".


Example: Calculate dependency boundaries
-------------------------------------

@
  build-depends:       aeson
                     , base >= 4.7 && < 5
@

When the upper and lower version boundary of a library dependency needs to be found, 
we need to know in which version of a library a function exists. We see a version
of a library as a set of functions with name, type and namespace (others properties 
left away for simplicity in this  example).
The correct boundaries come from an intersection of all sets, with equal functions in the
intersections. We can broaden this equality later to find name changes of functions, bug fixes,
and other things. The intersection of sets is nothing one should do again and again for every 
imported function. Therefore we insert edges into the graph that connect all functions that are 
equal from the lowest version to the highest where it appears.
If we import several functions we only have to follow these edges and find the lowest(highest) 
common version of all functions. Although following edges with judy arrays is also O(log n), it is 
faster than looking up strings of types and function-names in Data.Map. We also later want to do
structure comparison of functions only once. Thats why this is a handy post processing step to 
speed up queries.

To avoid a O(n²) brute force comparison of every node from set A to every node from Set B
(intersection), we sort all functions in a package after type and name: O(n log n), and insert them
again in the graph. This way of reconnecting the nodes also allows to find renaming of functions,
 namespaces when the type hasn't changed. Of course the type alone is not enough to find equality 
of two functions. In the real implementation we also compare the internal structure of the 
functions, but the filtering by the same type reduces the search space a lot.

-}
module Main where

import qualified Data.List.NonEmpty as NonEmpty
import           Data.Text(Text)
import           JudyGraph
import qualified JudyGraph as J

main :: IO ()
main = do
  jgraph <- J.fromListE False nodes dirEdges [] ranges :: IO (EnumGraph NodeLabel EdgeLabel)
--  [CN p, _, CN v, _, CN f, _] 
  query <- table jgraph True (f0 --| next |-- function) -- (packages --> packagesVer --> function)
--  createMem jgraph (p --> v --> appl sort f)
--  create jgraph dbPath (p --> v --> appl sort f)
  putStrLn ("query result: " ++ show query) -- show (p,v,f))
  putStrLn "Done"
 where
  packages    = node (nodes32 [0]) :: CyN -- (labels [PACKAGE ""]) :: CyN
  packagesVer = node (labels [PACKAGEVER ""]) :: CyN
  function    = node (labels [FUNCTION F]) :: CyN
  an = node anyNode :: CyN
  sort ns = ns
  f0 = node (nodes32 [7]) :: CyN
  next = edge (attr NextVer) (1…3) :: CyE -- (attr Closes) :: CyE

  nodes :: [(J.Node32, NodeLabel)]
  nodes = map n32n
          [(0, PACKAGE "test"),
           (1, PACKAGEVER "test-0.1"), (2, PACKAGEVER "test-0.2"), (3, PACKAGEVER "test-0.3"),
           (4, FUNCTION (Func "Int -> Bool" "odd"  "MyPrelude")),
           (5, FUNCTION (Func "Int -> Bool" "even" "MyPrelude")),
           (6, FUNCTION (Func "Int -> Int"  "bell" "Test.Speculate.Utils")),
           (7, FUNCTION (Func "Int" "a"  "MyPrelude")),
           (8, FUNCTION (Func "Bool" "b"  "MyPrelude")),
           (9, FUNCTION (Func "Double" "c"  "MyPrelude")),
           (10, FUNCTION (Func "Double" "c"  "MyPrelude"))
          ]

  -- ranges are normally generated automatically from graph files, but here we do it by hand
  ranges = NonEmpty.fromList [(0, PACKAGE ""), (1, PACKAGEVER ""), (4, FUNCTION F)]

  dirEdges :: [(J.Edge, Maybe NodeLabel, Maybe NodeLabel, [EdgeLabel], Bool)]
  dirEdges = map n32e
             [((0,1), [PartOf]), ((0,2), [PartOf]), ((0,3), [PartOf]),
              ((1,4), [PartOf]), ((2,4), [PartOf]), ((3,4), [PartOf]), -- "odd" is part of all three vers
                                 ((2,5), [PartOf]), ((3,5), [PartOf]), -- "even" exists since test-0.2
                                 ((2,6), [PartOf]), -- "bell" is only used in test-0.2
              ((1,2), [NextVer]), ((2,3), [NextVer]),
              ((7,8), [NextVer]),
              ((8,9), [NextVer]),
              ((9,10), [NextVer])
             ]

--------------------------------------------------------------------------------------------------

type CyN = CypherNode NodeLabel EdgeLabel
type CyE = CypherEdge NodeLabel EdgeLabel

data NodeLabel = PACKAGE Text | PACKAGEVER Text | FUNCTION Func deriving (Eq, Show)

instance Enum NodeLabel where
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

-- | Can be complex (like a record). Figure out which attributes are important for filtering edges
data EdgeLabel = PartOf | NextVer deriving Show

instance NodeAttribute NodeLabel where
    fastNodeAttr _ = (0, 0) -- we don't use node attrs

instance EdgeAttribute EdgeLabel where
    -- What a programmer can do
    fastEdgeAttr PartOf   = (8,0x1000001) -- take the 8 highest bits
    fastEdgeAttr NextVer  = (8,0x2000001) -- take the 8 highest bits
    fastEdgeAttrBase PartOf  = 0x1000000
    fastEdgeAttrBase NextVer = 0x2000000
    edgeForward _ = 0
--    addCsvLine _ graph _ = return graph

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

