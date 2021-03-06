{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}
{-|
Module      :  Main
Description :  Benchmarks for Judygraph
Copyright   :  (C) 2018 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

-}
module Main(main) where

--import Criterion(Benchmark, bench, nf)
import Criterion
import Criterion.Main (bgroup, defaultMain)
import qualified Data.Judy as J
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Word(Word8, Word16, Word32)
import GHC.Generics (Generic)
import qualified JudyGraph as J
import qualified JudyGraph.Cypher as Cy
import JudyGraph
import JudyGraph.Enum(insertNodeLines, emptyE, NodeAttribute(..), EdgeAttribute(..))
import Paths_judy_graph_db (getDataFileName)

-- Our benchmark harness.
main = defaultMain [
  bgroup "socialsensor/graphdb-benchmarks"
          [ bench "CW"    $ nfIO cw
          , bench "MIW"   $ whnfIO miw
--          , bench "SIW"   $ nfIO siw
--          , bench "QW-FN" $ nfIO qwfn
--          , bench "QW-FA" $ nfIO qwfa
--          , bench "QW-FS" $ nfIO qwfs
          ]
  ]

cw = do f <- getDataFileName "benchmark/data.txt"
        putStrLn ("getDataFileName " ++ f)
        jgraph <- emptyE ranges
        insertNodeLines jgraph f MAILED
        query <- table jgraph True (number128 --| mailed |-- anybody)
        putStrLn ("query result: " ++ show query)
  where
    ranges = NonEmpty.fromList [((0,1), (EMPLOYEE, [MAILED]))]
    number128 = node (nodes32 [128]) :: CyN
    anybody = node anyNode :: CyN
    mailed = edge (attr MAILED) :: CyE


miw = do f <- getDataFileName "benchmark/data.txt"
         jgraph <- emptyE ranges
         insertNodeLines jgraph f MAILED
         putStrLn ("insertNodeLines")
  where
    ranges = NonEmpty.fromList [((0,1), (EMPLOYEE, [MAILED]))]

------------------------------------------------------------------------

type CyN = CypherNode NodeLabel EdgeLabel
type CyE = CypherEdge NodeLabel EdgeLabel

data EdgeLabel = MAILED deriving (Show)

data NodeLabel = EMPLOYEE deriving (Show, Enum, Eq)

instance NodeAttribute NodeLabel where
    fastNodeAttr _ = (0, 0) -- we don't use node attrs

instance EdgeAttribute EdgeLabel where
    fastEdgeAttr MAILED     = (8,0x1000001) -- take the 8 highest bits
    fastEdgeAttrBase MAILED = 0x1000000
    edgeFromAttr (Edge32 0x1000000)  = MAILED

