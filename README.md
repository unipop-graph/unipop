_**`This project is in active development. A stable version will be released soon.`**_

# <img src="https://raw.githubusercontent.com/rmagen/unipop/master/docs/images/unipop-logo.png" width=70 style="vertical-align:middle;"> <span style="vertical-align:middle;">Unipop</span>

Unipop aims to simplify and improve Data Analytics and Business Intelligence.
- **Data Federation** - Analyze different sources of data together. No data migration is needed.
- **Data Virtualization** - Map data in different formats to a unified graph schema. No data restructuring is needed.
- **Graph Data Model** -  Unleash the power of Graphs using the
[Gremlin](http://tinkerpop.apache.org/gremlin.html) query language.
([SQL](https://github.com/twilmes/sql-gremlin) and
[SPARQL](https://github.com/dkuppitz/sparql-gremlin) are also available).


## Background
Most organisations have their data spread out in multiple Data Stores: RDBMS, Document Stores, Key-Value Stores,
Column-Family Store, Filesystems, Enterprise tools & services, etc. There are different reasons for this:
- Different projects
- SOA Architecture
- NoSql "Hybrid" solutions
- Legacy Databases
- Multiple version support

Unfortunately, the myriad of disparate **data stores**, coupled with different and always changing **schemas**, make it hard
to properly analyze **interconnected** data and get meaningful and useful information out of it.

Unipop aims to mitigate these difficulties.


## Why Gremlin?
**SQL**, the industry standard, is a fine tool for many use cases. However it does have its down sides:
- **Data Schema** - Structured. Tables and their fields need to be explicitly defined.
- **Data Connections** - Joining Relational tables requires explicit knowledge of all relationships (PK/FK),
and can become [quite complicated](http://sql2gremlin.com/#_recommendation). This inhibits exploration of your data.
- **Flexibility** - The exclusively Declarative nature of SQL confines it to very specific query structures.
- **Usability** - Queries are loosely-typed "free text", often requiring complicated ORMs.

**Gremlin** can better handle varied, interconnected, frequently changing data:
- **Data Schema** - Unstructured. It's not necessary to pre-define types and their properties. Graphs only have two types: Vertex & Edge.
- **Data Connections** - In a graph Edges are "First-class citizens". This allows free exploration of your data.
- **Flexibility** - Gremlin queries are written in a functional, pipeline-structured style,
providing considerable flexibility.
- **Usability** - Host Language embedding. Easier to read, write, find errors, and reuse queries.

**Apache Tinkerpop**, the Gremlin framework, also provides other useful features:
- **Query Planner** - extensible optimization mechanism (`TraversalStrategy`).
- **Console & Server** - production grade tooling.
- **Language Drivers** - JavaScript, TypeScript, PHP, Python, Java, Scala, .Net, Go.
- **Extensible Query Languages** - [Gremlin](http://tinkerpop.apache.org/gremlin.html),
[SQL](https://github.com/twilmes/sql-gremlin), [SPARQL](https://github.com/dkuppitz/sparql-gremlin)
- **Testing Framework**


## Getting started
*TBD*

#### Setup
- Console - a local instance with an interactive Shell for issuing queries.
- Server - a web server with WebSocket & HTTP APIs.
- Embedded - run Unipop inside any JVM based application.

#### Configure
Add your data sources to Unipop's configuration. Configuring a source entails mapping its schema to a "property
graph" model (i.e. vertices & edges).
Unipop is built in an extensible way, enabling many different mapping options.

#### Query
Console, Server, Embedded, or Language drivers

## Customize & Extend
*TBD*

## How it works
*TBD*

Technical details.

## Contributing
*TBD*