{-# LANGUAGE DeriveGeneric, OverloadedStrings #-}
{-|
Module      :  Cypher
Description :  Cypher like edsl to make queries on the graph
Copyright   :  (C) 2017 Tillmann Vogt

License     :  BSD-style (see the file LICENSE)
Maintainer  :  Tillmann Vogt <tillk.vogt@gmail.com>
Stability   :  provisional
Portability :  POSIX

Neo4j invented Cypher as a sort of SQL for their graph database. But it is much nicer to have an EDSL in
Haskell than a DSL that always misses a command. The ASCII art syntax could not be completely taken over, but it is very similar.
-}

module Graph.Cypher where

import qualified Data.Judy as J
import qualified Data.Map.Strict as Map
import           Data.Map.Strict(Map)
import           Data.Maybe(fromJust, isJust, isNothing, maybe, catMaybes, fromMaybe)
import qualified Data.Set as Set
import           Data.Set(Set)
import qualified Data.Text as T
import           Data.Text(Text)
import           Data.Word(Word8, Word16, Word32)
import Graph.FastAccess(JGraph(..), Judy(..))
import Debug.Trace

-- A little rant

-- Learning a new language with its syntax for every domain is a bad idea. The only 
-- good aspect about DSLs is that they glue a customer to the company.
-- I once needed a command to combine search results which wasn't fixed for more than a year in Neo4j.
-- That made me finally abandon Neo4j. Domain Specific languages that are not emedded are a 
-- mistake like programming "declaratively" with XML.


data CypherEdge = P String -- attribute string
                | V String -- variable length path: "*", "*1..5", "*..6"

data NodeType = Function | Type | App | Lit

data EncodedEdge = R | L | OutType | InType1_30 | NextArg


data CypherNode = Any

data CypherResult a = CyGraph Judy
                    | Rows [a] -- rows



executeOn :: JGraph nl el -> CypherNode -> IO (CypherResult a)
executeOn judyGraph cypher = do
    return (Rows [])


anyNode :: CypherNode
anyNode = Any


(--|) :: CypherNode -> CypherEdge -> CypherEdge
-- ^Half of an undirected edge from x
(--|) node edge = edge

(|--) :: CypherEdge -> CypherNode -> CypherNode
-- ^Half of an undirected edge to x
(|--) edge node = node

(<--|) :: CypherNode -> CypherEdge -> CypherEdge
-- ^Half of a directed edge from x
(<--|) node edge = edge

(|-->) :: CypherEdge -> CypherNode -> CypherNode
-- ^Half of an directed edge to x
(|-->) edge node = node


(-~-) :: CypherNode -> CypherNode -> CypherNode
-- ^An undirected edge
(-~-) node0 node1 = node0

(-->) :: CypherNode -> CypherNode -> CypherNode
-- ^A directed edge
(-->) node0 node1 = node0

(<--) :: CypherNode -> CypherNode -> CypherNode
-- ^A directed edge
(<--) node0 node1 = node0

infixl 7 --|
infixl 7 |--
infixl 7 <--|
infixl 7 |-->
infixl 7 -~-
infixl 7 -->
infixl 7 <--


----------------------------------------------------------
-- Cypher examples

exampleCypher = anyNode --| P "LIKES" |--> anyNode

--  executeOn

