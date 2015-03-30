# elastic-gremlin
[TinkerPop 3](http://www.tinkerpop.com/docs/3.0.0.M7/) implementation on Elasticsearch backend.

## Getting Started!
clone our project to your workspace
compile & build it using mvn command
```
mvn clean install -Dmaven.test.skip=true
```
### Gremlin Server
  1.  download the latest gremlin server from [here](http://tinkerpop.com/downloads/3.0.0.M7/gremlin-server-3.0.0.M7.zip)
  2.  add to gremlin-server\ext folder the files from \target\lib and ofcourse the elastic-gremlin-3.0.0.M7.jar from \target
  3.  configuration:
    * on gremlin-server.yaml put this configuration on graphs section: { g: conf/elastic-graph.properties}
    * create a file named elastic-graph.properties and put there key&values according to elastic-gremlin configuration
    for example:
    ```
    gremlin.graph=com.tinkerpop.gremlin.elastic.structure.ElasticGraph
    elasticsearch.client=TRANSPORT_CLIENT
    elasticsearch.cluster.address=127.0.0.1:9300
    elasticsearch.cluster.name=demo_elastic_gremlin_cluster
    elasticsearch.index.name=elastic_graph_index
    elasticsearch.refresh=false
    ```
  Now you can communicate with the gremlin-server through rest/gremlinserver api and watch the documents stored in your elasticsearch servers :)
  
### Gremlin Console
### Java API


