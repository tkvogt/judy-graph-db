module Main where

import           Data.Bits(shift)
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Word(Word8, Word16, Word32)

import JudyGraph
import qualified JudyGraph as J
import qualified JudyGraph.Cypher as Cy

main :: IO ()
main = do
  jgraph <- J.fromListE False nodes dirEdges [] ranges

  -- Which of the issues that simon has raised reference other issues?
  query <- table jgraph (simon --| raises |-- issue --| references |-- issue)
  putStrLn ("query result: " ++ show query)
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

  -- ranges are normally generated automatically from graph files, but here we do it by hand
  ranges = NonEmpty.fromList [(0, PROGRAMMER), (2, ORGANISATION), (3, ISSUE), (7, PULL_REQUEST)]

  dirEdges :: [(J.Edge, [EdgeLabel])]
  dirEdges = [((0,3), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((0,4), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((0,5), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((0,6), [Raises]),     -- PROGRAMMER Raises ISSUE
              ((4,3), [References]), -- ISSUE References ISSUE
              ((5,4), [Closes]),     -- PULL_REQUEST Closes ISSUE
              ((0,3), [Closes]),     -- PROGRAMMER Closes ISSUE
              ((1,7), [Accepts]),    -- PROGRAMMER Accepts PULL_REQUEST
              ((0,2), [BelongtsTO])] -- PROGRAMMER BelongtsTO ORGANISATION

--------------------------------------------------------------------------------------------------

type CyN = CypherNode NodeLabel EdgeLabel
type CyE = CypherEdge NodeLabel EdgeLabel

data NodeLabel = PROGRAMMER | ORGANISATION | ISSUE | PULL_REQUEST deriving (Eq, Show, Enum)

-- | Can be complex (like a record). Figure out which attributes are important for filtering edges
data EdgeLabel = Raises | Accepts | Closes | References | BelongtsTO | EdgeForward deriving Show

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
