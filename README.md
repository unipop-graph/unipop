_**`This project is in active development. A stable version will be released soon.`**_

# <img src="https://raw.githubusercontent.com/rmagen/unipop/master/docs/images/unipop-logo.png" width=70 style="vertical-align:middle;"> <span style="vertical-align:middle;">Unipop</span>
[![Build Status](https://travis-ci.org/unipop-graph/unipop.svg?branch=travisTest)](https://travis-ci.org/unipop-graph/unipop)

> Analyze data from **multiple sources** using the [power of
**graphs**](https://academy.datastax.com/resources/getting-started-graph-databases).

**Unipop** is a data [Federation](https://en.wikipedia.org/wiki/Federated_database_system)
and [Virtualization](https://en.wikipedia.org/wiki/Data_virtualization)
engine that models your data as a "virtual" graph,
exposing a querying API using the [Gremlin](http://tinkerpop.apache.org/gremlin.html) GQL
([Sql](https://github.com/twilmes/sql-gremlin)
and [SPARQL](https://github.com/dkuppitz/sparql-gremlin) are also available.)

This means you get the benefits of a graph data model without migrating/replicating/restructuring
your data, whether its stored in a ***RDBMS***, **_NoSql Store_**,
or any other data source (see "Customize and Extend" below.)


### Why Graphs?

Graphs provide a very "natural" way to analyze your data.
The simple Vertex/Edge structure makes it very easy to model complex and varied data,
and then analyze it by exploring the connections/relationships in it.

This is especially relevant for a data Federation / Virtualization platform,
which integrates a large variety of different data sources, structures, and schemas.


Our chosen GQL is **Gremlin**, which comes as part of the
[**Apache Tinkerpop**](http://tinkerpop.incubator.apache.org/) framework.
Let's compare Gremlin to _**SQL**_, the industry standard:

|           | Schema   | Relationships   | Flexibility   | Usability   |
|:---------:|----------|-----------------|---------------|-------------|
| SQL       | _Structured_ - Tables and their fields need to be explicitly defined. | Joins require knowledge of all relationships (PK/FK), and can become [quite complicated](http://sql2gremlin.com/#_recommendation). | Sql's syntax requires very specific, rigid structures. | Queries are loosely-typed "free text", often requiring complicated ORMs. |
| Gremlin   | _Unstructured_ - Different structures can be created on the fly. | Connections (i.e edges) are "First-class citizens", enabling easy exploration of your data. | Queries are written in a pipelined ("functional") syntax, providing considerable flexibility. | Host Language embedding. Easier to read, write, find errors, and reuse queries. |


The Tinkerpop framework also provides us with other useful features "out of the box":
- **Traversal Strategies** - an extensible query optimization mechanism.
Unipop utilizes this to implement different performance optimizations.
- **Console & Server** - production grade tooling.
- **Language Drivers** - JavaScript, TypeScript, PHP, Python, Java, Scala, .Net, Go.
- **Extensible Query Languages** - [Gremlin](http://tinkerpop.apache.org/gremlin.html),
[SQL](https://github.com/twilmes/sql-gremlin), [SPARQL](https://github.com/dkuppitz/sparql-gremlin)
- **DSL support**
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