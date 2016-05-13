# <img src="https://raw.githubusercontent.com/rmagen/unipop/master/docs/images/unipop-logo.png" width=70/> Unipop

**This project is in active development. The first stable version will be released soon.**

Most organisations have their data spread out in multiple data stores
- RDBMS
- Document Stores
- Key-Value Stores
- Column-Family Store
- Filesystems
- Enterprise tools & services
- etc

There are different reasons for this:
- Multiple projects
- NoSql "Hybrid" Architectures
- Multiple application versions
- Legacy Databases

Unfortunately, this makes it hard to query and reason about the relationships in your data.
Unipop offers a solution in the form of a **Federation** / **Data-Virtualization** platform.

### Why use Unipop?
There are other projects with similar goals
([Drill](http://drill.apache.org/), [Denodo](http://www.denodo.com/en), [Dremel](http://research.google.com/pubs/pub36632.html), etc.)

Usually these projects provide SQL as their querying API. SQL is fine for many use-cases, but can also pose some difficulties and obstacles:
- Hard-structured schemas
- Complicated relationship handling (i.e. [JOIN Hell](http://sql2gremlin.com/#_recommendation))
- Long, unreadable queries (e.g. Nested queries)
- Fully declarative, thus confining queries to very specific structures (versus a functional, "pipe-line" based language)
- More...

Unipop uses [Gremlin](http://tinkerpop.apache.org/gremlin.html) as its default query language.
Gremlin is a functional query language for graphs, providing an easy and natural way to query your data.


### Getting started

#### Setup
- Console - a local instance with an interactive Shell for issuing queries.
- Server - a web server with WebSocket & HTTP APIs. Includes drivers for JS, TS, PHP, Python, Java, Go, Scala, .Net.
- Embedded - run Unipop inside any JVM based application.

#### Configure
Add your data sources to Unipop's configuration. Configuring a source includes mapping its schema to a "property graph" (vertices & edges).
Unipop is built in an extensible way, enabling many different mapping options.

#### Query
- [Gremlin](http://tinkerpop.apache.org/gremlin.html)
- [SPARQL](https://github.com/dkuppitz/sparql-gremlin)
- [SQL](https://github.com/twilmes/sql-gremlin)


### Advanced

#### Customizing & Extending Unipop
**TBD**

#### How it works
Technical details.
**TBD**

#### Contributing
**TBD**