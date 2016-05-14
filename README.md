_**`This project is in active development. A stable version will be released soon.`**_

# <img src="https://raw.githubusercontent.com/rmagen/unipop/master/docs/images/unipop-logo.png" width=70 style="vertical-align:middle;"> <span style="vertical-align:middle;">Unipop</span>

Unipop aims to simplify and improve Data Analytics and Business Intelligence
- **Data Federation** - Analyze different sources of data together. No data migration is needed.
- **Data Virtualization** - Map data in different formats to a unified graph schema. No data restructuring is needed.
- **Graph Data Model** -  Unleash the power of Graphs using the
[Gremlin](http://tinkerpop.apache.org/gremlin.html) query language.
([SQL](https://github.com/twilmes/sql-gremlin) and
[SPARQL](https://github.com/dkuppitz/sparql-gremlin) are also available).

###Background
Most organisations have their data spread out in multiple Data Stores: RDBMS, Document Stores, Key-Value Stores,
Column-Family Store, Filesystems, Enterprise tools & services, etc. There are different reasons for this:
- Different projects
- SOA Architecture
- NoSql "Hybrid" solutions
- Legacy Databases
- Multiple version support

Unfortunately, the myriad of disparate Data Stores, coupled with different and always changing Schemas, make it hard
to properly analyze your data and get any meaningful and useful information out of it.

Unipop aims to mitigate these difficulties.

### Why Gremlin?
**SQL**, the industry standard, is a fine tool for many use cases. However it does have its down sides:
- Structured Schema - Tables and their fields need to be explicitly defined.
- Relationships - The Relational Data Model can quickly become a [JOIN nightmare](http://sql2gremlin.com/#_recommendation).
- Declarative - The exclusively declarative nature of SQL confines it to very specific query structures.
- Usability - Queries are loosely-typed "free text", often requiring complicated ORMs.

**Gremlin** can better handle varied, multi-connected, and frequently changing data:
- Unstructured Schema - It's not necessary to pre-define types or their properties. Elements are either a Vertex or Edge.
- Connections - Connections (i.e. Edges) are an integral part of a Graph Data Model and the Gremlin query language.
- Imperative _and_ Declarative - Gremlin queries are written in a functional, pipeline-styled structure,
providing considerable flexibility in queries.
- Usability - Host Language embedding. Easier to read, write, find errors, and reuse query code.

**Apache Tinkerpop**, the Gremlin framework, also provides other useful features:
- Query optimization
- Additional Query Languages
- Language Drivers
- Console & Server implementations
- Testing Framework

## Getting started
*TBD*

### Setup
- Console - a local instance with an interactive Shell for issuing queries.
- Server - a web server with WebSocket & HTTP APIs. Includes drivers for JS, TS, PHP, Python, Java, Go, Scala, .Net.
- Embedded - run Unipop inside any JVM based application.

#### Query

#### Configure
Add your data sources to Unipop's configuration. Configuring a source includes mapping its schema to a "property
graph" (vertices & edges).
Unipop is built in an extensible way, enabling many different mapping options.

## More
*TBD*

#### Customize & Extend


#### How it works
Technical details.

#### Contributing