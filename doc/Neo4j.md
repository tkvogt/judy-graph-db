# Comparison with Neo4j#

It is possible to replace Neo4j commands with native Haskell functions.<br>
There are of course differences because an EDSL is slightly less comfortable than the Neo4j DSL. <br>
This is taken from https://neo4j.com/docs/cypher-refcard/current/ .

## PATTERNS ##

#### Neo4j ####
``` (n:Person) ```
Node with Person label.

#### judy-graph-db ####
```haskell
node (labels [Person])
```
If you want to reuse the node use:
```haskell
   n where n = node (labels [Person])
```

---

#### Neo4j ####
``` (n:Person:Swedish) ```
Node with both Person and Swedish label.

#### judy-graph-db ####
```haskell
node (labels [SwedishPerson])
```
In judyDB the labels partition the nodes. So every node is in only one partition. This has advantages in speed and memory efficiency and is good enough in most cases. If it should not be enough use where_ clause and query the secondary structure (having created it at the beginning).

---

#### Neo4j ####
```(n:Person {name: $value})```
Node with the declared properties.

#### judy-graph-db ####
```haskell
node (labels [Person]) (where_ filt)
  where filt x = name x == "value"
```

---

#### Neo4j ####
```()-[r {name: $value}]-()```
Matches relationships with the declared properties.

#### judy-graph-db ####
```haskell
  node anyNode --| named |-- node anyNode
    where named = edge (whereE filt)
          filt x = name x == "value"
```

---

#### Neo4j ####
``` (m)-->(n) ```
Relationship from m to n.

#### judy-graph-db ####
```haskell
  m --> n
    where m = node anyNode
          n = node anyNode
```
You have to say what m and n are

---

#### Neo4j ####
``` (m)-->(n) ```
Relationship from m to n.

#### judy-graph-db ####
```haskell
m ~~ n
  where m = node anyNode
        n = node anyNode
```
Like above, but undirected. Haskell uses ```--``` for comments, so we have to use tildes: ```~~```

---

#### Neo4j ####
``` (m:Person)-->(n) ```
Node m labeled Person with relationship to n.
#### judy-graph-db ####
```haskell
m --> n
  where m = node (labels [Person])
        n = node anyNode
```

---

#### Neo4j ####
``` (m)<-[:KNOWS]-(n) ```
Relationship of type KNOWS from m to n.

#### judy-graph-db ####
```haskell
m <--| knows |-- n
  where knows = edge (attr KNOWS)
```

---

#### Neo4j ####
``` (m)<-[:KNOWS|:LOVES]-(n) ```
Relationship of type KNOWS or of type LOVES from m to n.

#### judy-graph-db ####
```haskell
m <--| knowsLoves |-- n
  where knowsLoves = edge (attr KNOWS) (attr LOVES)
```

---

#### Neo4j ####
``` (m)-[r]->(n) ```
Bind the relationship to variable r.

#### judy-graph-db ####
```haskell
m <--| r |-- n
  where r = edge (whereE )
```
? If you bind it to something then you use it to somehow restrict the edges used.

---

#### Neo4j ####
``` (m)-[*1..5]->(n) ```
Variable length (between 1 and 5) path in relationships from m to n.

#### judy-graph-db ####
```haskell
m <--| r |-- n
  where r = edge (1…5)
```

---

#### Neo4j ####
``` (m)-[*]->(n) ```
Variable length path of any number of relationships from m to n.

#### judy-graph-db ####
```haskell
m <--| r |-- n
  where r = edge (1…4294967295)
```
Enough? I'll add another more elegant function if needed. Curretly I never needed this.

---

#### Neo4j ####
``` (m)-[:KNOWS]->(n {property: $value}) ```
A relationship of type KNOWS from a node m to a node n with the declared property.

#### judy-graph-db ####
```haskell
TODO
```

---

#### Neo4j ####
``` shortestPath((n1:Person)-[*..6]-(n2:Person)) ```
Find a single shortest path.

#### judy-graph-db ####
```haskell
TODO
```

---

#### Neo4j ####
``` allShortestPath((n1:Person)-[*..6]-(n2:Person)) ```
Find all shortest paths.

#### judy-graph-db ####
```haskell
TODO
```

---

#### Neo4j ####
``` size((n)-->()-->()) ```
Count the paths matching the pattern.

#### judy-graph-db ####
```haskell
TODO
```

---

## Read Query Structure ##

### MATCH ###

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| ``` MATCH (n:Person)-[:KNOWS]->(m:Person) ``` <br> WHERE n.name = 'Alice' <br> Node patterns can contain labels and properties. | | |
| ``` MATCH (n)-->(m) ``` <br> Any pattern can be used in MATCH. | | |
| ``` MATCH (n {name: 'Alice'})-->(m) ``` <br> Patterns with node properties. | | |
| ``` MATCH p = (n)-->(m) ``` <br> Assign a path to p. | | |
| ```OPTIONAL MATCH (n)-[r]->(m) ``` <br> Optional pattern: nulls will be used for missing parts. | | |

### WHERE ###

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| WHERE n.property <> $value <br> Use a predicate to filter. Note that WHERE is always part of <br> a MATCH, OPTIONAL MATCH, WITH or START clause. Putting it after <br> a different clause in a query will alter what it does. | | |

### RETURN ###

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| RETURN *  <br> Return the value of all variables. | | |
| RETURN n AS columnName <br> Use alias for result column name. | | |
| RETURN DISTINCT n <br> Return unique rows. | | |
| ORDER BY n.property <br> Sort the result. | | |
| ORDER BY n.property DESC <br> Sort the result in descending order. | | |
| SKIP $skipNumber <br> Skip a number of results. | | |
| LIMIT $limitNumber <br> Limit the number of results. | | |
| SKIP $skipNumber LIMIT $limitNumber <br> Skip results at the top and limit the number of results. | | |
| RETURN count(*) <br> The number of matching rows. <br> See Aggregating Functions for more. | | |

### WITH ###

### UNION ###

## Read-Write Query Structure ##

### CREATE ###

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| CREATE (n {name: $value}) <br> Create a node with the given properties. | | |
| CREATE (n $map) <br> Create a node with the given properties. | | |
| UNWIND $listOfMaps AS properties <br> CREATE (n) SET n = properties <br> Create nodes with the given properties. | | |
| CREATE (n)-[r:KNOWS]->(m) <br> Create a relationship with the given type and direction; bind a variable to it. | | |
| CREATE (n)-[:LOVES {since: $value}]->(m) <br> Create a relationship with the given type, direction, and properties. | | |

### SET ###

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| SET n.property1 = $value1, <br> n.property2 = $value2 <br> Update or create a property. | | |
| SET n = $map <br> Set all properties. This will remove any existing properties. | | |
| SET n += $map <br> Add and update properties, while keeping existing ones. | | |
| SET n:Person <br> Adds a label Person to a node. | | |

### IMPORT ###

### MERGE ###

### DELETE ###

### REMOVE ###

### FOREACH ###

### CALL ###

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| | | |

# Return #

| Neo4j         | judy-graph-db | Comment |
| ------------- |:-------------:| -------:|
| | | |

