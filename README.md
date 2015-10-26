# Unipop
Run Graph queries on your existing databases.

Most organisations have multiple sources of data: RDBMSs, Document Stores, KV Stores, special enterprise software, filesystems, etc. Usually you'll have a whole bunch of different ones. 

Of course there's nothing wrong with diversity (different tools for different jobs...), but spreading data around makes it hard to query and to reason about the relationships in your data.

Unipop does not store data. Unipop connects to your databases, creates a model of your data and the relationships between your data, and enables you to easily query it using Gremlin.

[Tinkerpop Gremlin](http://tinkerpop.incubator.apache.org/docs/3.0.1-incubating/) is a functional language that enables easy querying of data modeled as a graph.

There are other projects with similiar goals as Unipop ([Drill](http://drill.apache.org/), [Calcite](https://calcite.incubator.apache.org/), [Dremel](http://research.google.com/pubs/pub36632.html)). These projects utilize SQL as their query language, which is fine for simple queries, but can quickly become a [JOIN nightmare](http://sql2gremlin.com/#_recommendation) when making complicated queries.
OTOH, Gremlin enables a very easy and natural way to query your data.

#How Does it work?

##Controllers

The basic building blocks of a graph are Vertices and Edges.

The basic building blocks of Unipop are VertexControllers and EdgeControllers.

```java
public interface VertexController {
    Iterator<BaseVertex> vertices(Object[] ids);
    Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics);
    BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel);
    BaseVertex addVertex(Object id, String label, Object[] properties);
}

public interface EdgeController {
    Iterator<BaseEdge> edges(Object[] ids);
    Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics);
    Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics);
    BaseEdge addEdge(Object edgeId, String label,Vertex outV, Vertex inV, Object[] properties);
}
```

Controllers are responsible for querying data and transforming the results to vertices and edges.
Lets say we have data stored in Elasticsearch that we want to represent as vertices. We'll use an ElasticVertexController, which queries ES documents and converts them to vertices.
But we can go a step further. Lets say that those documents have an email address in them, and we want to connect to their LinkedIn profile. First we would use a RESTVertexController in order to retrieve the profiles as vertices, and then we'd extend ElasticVertexController to represent the email field as an edge to a profile vertex (implementing both VertexController & EdgeController).

We're bundling with Unipop a few generic Controllers:

- ElasticSearch
  - ElasticVertexController - ES documents as vertices.
  - ElasticEdgeController - ES documents as edges.
  - ElasticStarController(WIP) - ES documents as vertices, with nested documents as edges.
  - ElasticAggregationEdgeController(TBD) - ES Aggregation query, with the results represented as edges.
  - GeoIntersectsEdgeController(WIP) - Issues an ES spatial intersection query in order to connect two intersecting vertices. 
- ElasticSearch2
  - ElasticVertexController(WIP) - ES documents as vertices.
  - ElasticEdgeController(WIP) - ES documents as edges.
- JDBC
  - SqlVertex(WIP) - Runs an arbitrary sql command. Each row is transformed to a vertex.
  - SqlEdge(WIP) - Runs an arbitrary sql command. Each row is transformed to an edge.

And of course you can also implement your own Controllers (and submit useful ones back to us!). The options are endless. 

##ControllerManager






