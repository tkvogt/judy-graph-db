module Main where

import           Data.Bits((.&.))
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Word(Word8, Word16, Word32)

import qualified JudyGraph as Graph
import JudyGraph
import JudyGraph.FastAccess

main :: IO ()
main = do
  jgraph <- Graph.fromList nodes edges ranges
  query <- table jgraph (startNode --| raises |--> issue --| references |--> issue)
  putStrLn ("query result: " ++ show query)
 where
  startNode = Graph.nodes [0] :: CypherNode NodeLabel EdgeLabel
  raises = addAttr Attr Raises edge :: CypherEdge NodeLabel EdgeLabel
  issue = Graph.anyNode :: CypherNode NodeLabel EdgeLabel
  references = addAttr Attr References edge :: CypherEdge NodeLabel EdgeLabel

  nodes :: [(Graph.Node, NodeLabel)]
  nodes = [(0, PROGRAMMER), (1, PROGRAMMER),
           (2, ORGANISATION),
           (3, ISSUE), (4, ISSUE),
           (5, PULL_REQUEST)]

  edges :: [(Graph.Edge, [EdgeLabel])]
  edges = [((0,3), [Raises]),     -- PROGRAMMER Raises ISSUE
           ((0,4), [Raises]),     -- PROGRAMMER Raises ISSUE
           ((4,3), [References]), -- ISSUE References ISSUE
           ((5,4), [References]), -- PULL_REQUEST Closes ISSUE
           ((0,3), [Closes]),     -- PROGRAMMER Closes ISSUE
           ((1,5), [Accepts]),    -- PROGRAMMER Accepts PULL_REQUEST
           ((0,2), [BelongtsTO])] -- PROGRAMMER BelongtsTO ORGANISATION

  -- ranges are normally generated automatically from graph files, but here we do it by hand
  ranges = NonEmpty.fromList [(0, PROGRAMMER), (2, ORGANISATION), (3, ISSUE), (5, PULL_REQUEST)]

--------------------------------------------------------------------

data NodeLabel = PROGRAMMER | ORGANISATION | ISSUE | PULL_REQUEST deriving Show

-- | Can be complex (like a record). Figure out which attributes are important for filtering edges
data EdgeLabel = Raises | Accepts | Closes | References | BelongtsTO deriving Show

instance NodeAttribute NodeLabel where
    fastNodeAttr PROGRAMMER = (8, 1) -- take 8 leading bits
    fastNodeAttr ORGANISATION = (8, 2)
    fastNodeAttr ISSUE = (8, 3)
    fastNodeAttr PULL_REQUEST = (8, 4)

instance EdgeAttribute EdgeLabel where
    fastEdgeAttr Raises     = (31,1)
    fastEdgeAttr Accepts    = (31,2)
    fastEdgeAttr Closes     = (31,3)
    fastEdgeAttr References = (31,4)
    fastEdgeAttr BelongtsTO = (31,5)

    addCsvLine _ graph _ = return graph

