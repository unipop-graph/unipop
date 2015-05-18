# elastic-gremlin
[TinkerPop 3](http://tinkerpop.incubator.apache.org/docs/3.0.0-SNAPSHOT/) implementation on Elasticsearch backend. You should probably read up on that before proceeding.

### Features   
- **Huge graphs** - elastic-gremlin utilizes ES's great scale-out capabilities. 
That means your graph can scale onto lots of nodes!
We also provide a way to choose the ES indices and routing parameters to better optimize for your specific use-case (See IndexProvider).
- **Great indexing** -  We enable full usage of ES indexing. Either let elastic-search automaticaly create them, or cofigure the mappings for your specific needs. You can index:
  - Text (you can utilize ES's great analyzers)
  - Numbers
  - Dates
  - Geo (just use the Geo predicate in a 'has' clause)
 
  elastic-gremlin utilizes these indices when using 'has' steps.  
- **Aggregations** (Coming Soon)
  Aggregation traversals (e.g. g.V().count()) can benefit greatly from ES's [Aggregation module] (https://www.elastic.co/guide/en/elasticsearch/reference/1.x/search-aggregations.html)
  
Be warned, elastic-gremlin is still very much in development!


### Getting Started!
1. clone & build Tinkerpop 3.0.0.M9-incubating-rc2 from [here] (https://git-wip-us.apache.org/repos/asf?p=incubator-tinkerpop.git;a=shortlog;h=refs/tags/3.0.0.M9-incubating-rc2)
2. clone & build elastic-gremlin
    mvn clean install -Dmaven.test.skip=true
3. Create an ElasticGraph
    ```java
    BaseConfiguration config = new BaseConfiguration();
    /* put configuration properties as you like*/
    ElasticGraph graph = new ElasticGraph(config);
    GraphTraversalSource g = graph.traversal();
    g.addV();
    ```
4. Or just use the Gremlin Server or Gremlin Console


### Confiuration

##### `elasticsearch.client` (Default: "NODE")
The client type used to connect to elasticsearch. 
  - **NODE** Sets up a local elasticsearch node and runs against it. elastic-gremlin defaults to NODE, so you can get up and running as quickly as possible.
  - **TRANSPORT_CLIENT"** connects to an existing node.
  - **NODE_CLIENT** An optimized way to connect to an ES cluster. 

For more information read [here](http://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)

##### `elasticsearch.cluster.name`(Default: "elasticsearch")
The elasticsearch cluster's name.
 
##### `elasticsearch.cluster.address` (Default: "127.0.0.1:9300") 
The elasticsearch nodes' address. The format is: "ip1:port1,ip2:port2,...".

##### `elasticsearch.refresh` (Default: true) 
Whether to refresh the ES index before every search. Useful for testing.

##### `elasticsearch.indexProvider` (Default: "DefaultIndexProvider")
Accepts a name of a class implementing the IndexProvider interface. This interface is used to get the ES index & routing for every CRUD operation on an element. 

It also provides a way to create a new index if needed. You can easly implement your own IndexProvider and use it to create a "time based partition index" for some elements, store different elements on different indices , create your index with special configurations (like analyzers), etc.. 
elastic-gremlin comes with DefaultIndexProvider, which only uses one index for all the data.

##### `elasticsearch.index.name` (Default: "graph")
The elasticsearch index. For use together with DefaultIndexProvider.


### Bulk loading - add documentation


### Gremlin Server - outdated instructions!
  1.  download the latest gremlin server from [here](http://tinkerpop.com/downloads/3.0.0.M7/gremlin-server-3.0.0.M7.zip)
  2.  add to gremlin-server\ext folder the files from \target\lib and of course the elastic-gremlin-3.0.0.M7.jar from \target
  3.  configuration:
    * on gremlin-server.yaml put this configuration on graphs section: { g: conf/elastic-graph.properties}
    * create a file named elastic-graph.properties and put there key&values according to elastic-gremlin configuration
    examples.
    to connect to running cluster on remote address use this configuration (replace the address,cluster_name and index)
    ```
    gremlin.graph=com.tinkerpop.gremlin.elastic.structure.ElasticGraph
    elasticsearch.client=TRANSPORT_CLIENT
    elasticsearch.cluster.address=127.0.0.1:9300
    elasticsearch.cluster.name=demo_elastic_gremlin_cluster
    elasticsearch.index.name=elastic_graph_index
    elasticsearch.refresh=false
    ```
    In order to create an elastic backend through the gremlinServer (data will be saved on gremlin-server/data)
    use this configuration:
    ```
    gremlin.graph=com.tinkerpop.gremlin.elastic.structure.ElasticGraph
    elasticsearch.client=NODE
    elasticsearch.cluster.name=demo_elastic_gremlin_cluster
    elasticsearch.index.name=elastic_graph_index
    elasticsearch.refresh=false

    ```
  4.Run the gremlin-server/bin/gremlin-server
    now you can communicate with the gremlin-server through rest/gremlinserver api and watch the documents stored in your elasticsearch servers :)
  
  
### Gremlin Console - outdated instructions!
  1.  download the latest gremlin console from [here](http://tinkerpop.com/downloads/3.0.0.M7/gremlin-console-3.0.0.M7.zip)
  2.  add to gremlin-console\ext folder the files from \target\lib and of course the elastic-gremlin-3.0.0.M7.jar from \target
  3. run the gremlin-console\bin\gremlin file
  4. run the following commands
    ```
    
    import com.tinkerpop.gremlin.elastic.structure.ElasticGraph
    g = new ElasticGraph(new BaseConfiguration())
    
    ```
    
    of course you can put items on configuration like on server and load it.
######when we will release a stable version we will create a plugin and put it on maven. so all you will have to do is  :install com.tinkerpop elastic-gremlin 3.0.0 



