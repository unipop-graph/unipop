# <img src="https://raw.githubusercontent.com/rmagen/unipop/master/docs/images/unipop-logo.png" width=70/> Unipop

**This project is in active development. The first stable version will be released soon.**

Unipop is a tool meant to simplify and boost Data Analytics and Business Intelligence, by providing a streamlined way to query your data.
- **Data Federation** - Analyze different sources of data together. No data migration is needed.
- **Data Virtualization** - Map data in different formats to a unified graph schema. No data restructuring needed.
- **Graph Model** -  Unleash the power of Graphs using the
[Gremlin](http://tinkerpop.apache.org/gremlin.html) query language.
([SQL](https://github.com/twilmes/sql-gremlin) and
[SPARQL](https://github.com/dkuppitz/sparql-gremlin) are also available).

###Background
Most organisations have their data spread out in multiple Data Stores: RDBMS, Document Stores, Key-Value Stores,
Column-Family Store, Filesystems, Enterprise tools & services, etc.

There are different reasons for this:
- Different projects
- SOA Architecture
- NoSql "Hybrid" solutions
- Legacy Databases
- Multiple version support

Unfortunately, this myriad of disparate Data Stores, coupled with different and always changing Schemas, make it hard
to properly analyze your data and get any meaningful and useful information out of it.

Unipop aims to mitigate these difficulties.

### Why Gremlin?
**SQL** is a fine tool for many use-cases. However, it does have its down-sides:
- Structured Schema - inhibits variance in your data.
- Relationships - analyzing relationships with a relational data model can quickly become a [JOIN nightmare](http://sql2gremlin.com/#_recommendation).
- Readability - advanced queries can become long and complicated.
- Declarative - the exclusively declarative nature of SQL confines it to very specific query structures.

**Gremlin** can better handle varied, multi-connected, and frequently changing data:
- Dynamic Schema
- Relationships - connections are an integral part of a Graph data model (Edges), and the Gremlin language.
- Readability - easier to read, write, and reuse query code.
- Imperative & Declarative - Gremlin provides a functional, pipeline-styled query structure, enabling considerable flexibility in queries.


## Getting started

### Setup
- Console - a local instance with an interactive Shell for issuing queries.
- Server - a web server with WebSocket & HTTP APIs. Includes drivers for JS, TS, PHP, Python, Java, Go, Scala, .Net.
- Embedded - run Unipop inside any JVM based application.

#### Query
*TBD*

#### Configure
Add your data sources to Unipop's configuration. Configuring a source includes mapping its schema to a "property
graph" (vertices & edges).
Unipop is built in an extensible way, enabling many different mapping options.

## More

#### Customize & Extend
*TBD*

#### How it works
Technical details.
*TBD*

#### Contributing
*TBD*