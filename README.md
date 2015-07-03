# elastic-gremlin

[TinkerPop 3](http://tinkerpop.incubator.apache.org/docs/3.0.0-SNAPSHOT/) implementation on Elasticsearch backend. You should read up on Tinkerpop before you use elastic-gremlin.

## Features   
- **Scalable** <br> 
   elastic-gremlin utilizes ES's great scale-out capabilities. What that basically means is that your graph can spread out to many nodes, enabling more data while retaining good performance.⋅We also provide a way to choose the ES indices and routing parameters to better optimize for your specific use-case (See IndexProvider).
- **Indexing** <br>
We utilise ES's great indexing abilities. Either let elastic-gremlin automaticaly create them, or cofigure the mappings for your specific needs. You can index all kinds of things: Text (including analyzers), Numbers, Dates, Geo (just use the Geo predicate in a 'has' clause)
- **Custom Schema** <br>
You can customize the way your data is stored, enabling you to optimize it to your most common querying needs. Get the most out of ES by using its different Data Models for your specific needs (Nested Objects, Parent-Child Relationship, etc).<br>
You can also utilize this ability to query existing data in ElasticSearch, mapping it to with vertex-edge relationships.
- **Aggregations** (Coming Soon) <br>
Aggregation traversals (e.g. g.V().count()) can benefit greatly from ES's [Aggregation module](https://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-aggregations.html)


## Getting Started!
1. clone & build [Tinkerpop 3.0.0-SNAPSHOT](https://github.com/apache/incubator-tinkerpop/tree/master)
2. clone & build elastic-gremlin
    
    ```mvn clean install -Dmaven.test.skip=true```
3. Create an ElasticGraph:
   
    ```java
    BaseConfiguration config = new BaseConfiguration();
    /* put configuration properties as you like*/
    ElasticGraph graph = new ElasticGraph(config);
    GraphTraversalSource g = graph.traversal();
    g.addV();
    ```
4. Or just use the Gremlin Server or Gremlin Console.


##Confiuration

####Basic
Basic usage of elastic-gremlin creates or uses an existing ES index, with each Vertex and Edge contained in its own document.
You can customize some of the behaviour:

- `elasticsearch.client` (Default: "NODE") <br>
   The client type used to connect to elasticsearch. 
  - `NODE` Sets up a local elasticsearch node and runs against it. elastic-gremlin defaults to NODE, so you can get up and running as quickly as possible.
  - `TRANSPORT_CLIENT` Connects to an existing ES node.
  - `NODE_CLIENT` An optimized way to connect to an ES cluster. 
For more information read [here](http://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)
- `elasticsearch.cluster.name`(Default: "elasticsearch")<br>
The elasticsearch cluster's name.
- `elasticsearch.cluster.address` (Default: "127.0.0.1:9300") <br>
The elasticsearch nodes' address. The format is: "ip1:port1,ip2:port2,...".
- `elasticsearch.refresh` (Default: true) <br>
Whether to refresh the ES index before every search. Useful for testing.
- `elasticsearch.index.name` (Default: "graph")<br>
The elasticsearch index. For use together with DefaultIndexProvider.


####Advanced
Implement `QueryHandler` to use a customized schema that works best for your data. <br>
We still don't have enough documentation on this, but you can take a look at the implementations of `SimpleQueryHandler` and `ModernGraphQueryHandler`



You're welcome to send us any comments or questions (rmagen@gmail.com)



