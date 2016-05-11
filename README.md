<img src="https://raw.githubusercontent.com/rmagen/unipop/master/docs/images/unipop-logo.png" width=150/>

# Unipop
Run Graph queries on your existing databases.

Most organisations have multiple sources of data: RDBMSs, Document Stores, KV Stores, special enterprise software, filesystems, etc. Usually you'll have a whole bunch of different ones. 

Of course there's nothing wrong with diversity (different tools for different jobs...), but spreading data around makes it hard to query and to reason about the relationships in your data.

Unipop does not store data. Unipop connects to your databases, creates a model of your data and the relationships between your data, and enables you to easily query it using Gremlin.

[Tinkerpop Gremlin](http://tinkerpop.incubator.apache.org/docs/3.0.1-incubating/) is a functional language that enables easy querying of data modeled as a graph.

There are other projects with similar goals as Unipop ([Drill](http://drill.apache.org/), [Dremel](http://research.google.com/pubs/pub36632.html)).
These projects utilize SQL as their query language, which is fine for simple queries, but can quickly become a [JOIN nightmare](http://sql2gremlin.com/#_recommendation) when making complicated queries.
OTOH, Gremlin enables a very easy and natural way to query your data.

# Getting Sterted
**TBD**

# Configuring the Schema
**TBD**

# Customizing & Extending Unipop
**TBD**




