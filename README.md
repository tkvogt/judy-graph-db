DISCLAIMER: There are still a lot of TODOS, the API will change and I haven't done a lot of benchmarking.

Intro
=====

judy-graph-db is a graph database based on [judy arrays](https://en.wikipedia.org/wiki/Judy_array). It was developed because there was no Haskell library that could handle very dense graphs with a million edges coming from a node. It currently is focused on analysing a static set of graph files (like the panama papers) and then to query it and do a little bit of post processing like adding or updating edges.

judy-graph-db should be
 - fast: Because of judy-arrays
 - typesave and convenient: The Cypher-like query EDSL (Embedded Domain Specific Language) inspired by Neo4j eg enforces node/edge alternation. An EDSL has the advantage that we don't need to invent a big language like Cypher. There will never be a book written about this library, which IMHO what convenience is really about. Look at migration from Neo4j.
 - memory efficient: nodes are represented with Word32 indexes, edges also with Word32, if possible. Typeclasses are used to compress speed relevant properties into 32 bit.
 - flexible: Several typeclass graph instances balance between speed, memory efficiency and convenience
 - transparent: We explain all algorithms, and because of Haskell the library is easy to extend (if you are a Haskell programmer). As we use no monad apart from the IO-monad, there is only basic Haskell knowledge necessary.


On the downside (currently):
 - Deletion not tested and probably slows down the queries
 - No persistency yet
 - No thoughts on concurrency yet
 - No REST API yet (wich is maybe good for typesafety and: No standard passwords like MongoDB)
 - Cannot handle graphs that don't fit into memory
 - Judy Arrays are in IO. It is a binding to a C libary that is not easy to understand.

Overview
===========

When a query is executed, an algorithm typically doesn't need to access all parts of a graph. It would be ideal if the programmer could influence where parts of the graph end up: L1/L2/L3-Cache, memory or HD/SSD.

<img src="doc/idea.png" width="600">


Judy Arrays
===========

<img src="doc/judy.png" width="600">

Graph Types
=================

JGraph
------


EnumGraph
---------


ComplexGraph
------------



