# elastic-gremlin
[TinkerPop 3](http://www.tinkerpop.com/docs/3.0.0.M7/) implementation on Elasticsearch backend.
## Elastic-gremlin configuration properties

| Property| Default Value| Description      |
| --------| ------- | ------------------      |  
| `elasticsearch.client`| "NODE" | The client type used when connecting to elastic. options are: "NODE", "NODE_CLIENT" and  "TRANSPORT_CLIENT" . for more information read [here](http://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)          |
| `elasticsearch.cluster.name`  | "elasticsearch" | The elasticsearch cluster you are connecting too              |
| `elasticsearch.index.name` | "graph" | The index on elasticsearch on which elements will be inserted |
| `elasticsearch.cluster.address`  | "127.0.0.1:9300" | The elasticsearch address you are connecting too.The format is: "ip1:port1,ip2:port2,...".  You can add one or more by using "," . it is used on TransportClient settings              |
|`elasticsearch.refresh` | true | whether to refresh the index after each insert/update operation. good for testing and to ensure consistency. |
|`elasticsearch.batch`| false | when using this option , addVertex / addEdge operations does not make effect on your elasticsearch cluster until you do: g.commit() . On g.commit() a bulk request will be send to elastic. when using this option make sure to set elasticsearch.refresh to false  |
|`elasticsearch.schemaProvider` | DefaultSchemaProvider | SchemaProvider is used to figure the index name / routing for each insert/update/search of an element. it is also provides how to create a new index if needed. you can easly implement your own SchemaProvider ( it is an interface ) and use it to create a "time based partition index" for some elements, store different elements on different indices , create your index with special configurations (like analyzers) etc.. |

## Getting Started!
clone our project to your workspace
compile & build it using mvn command
```
mvn clean install -Dmaven.test.skip=true
```
### Gremlin Server
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
  
### Gremlin Console
  1.  download the latest gremlin console from [here](http://tinkerpop.com/downloads/3.0.0.M7/gremlin-console-3.0.0.M7.zip)
  2.  add to gremlin-console\ext folder the files from \target\lib and of course the elastic-gremlin-3.0.0.M7.jar from \target
  3. run the gremlin-console\bin\gremlin file
  4. run the following commands
    ```
    
    import com.tinkerpop.gremlin.elastic.structure.ElasticGraph
    g = new ElasticGraph(new BaseConfiguration())
    
    ```
    
    of course you can put items on configuration like on server and load it.
######when we will release a stable version we will create a plugin and put it on maven. so all you will have to do is  :install com.tinkerpop elastic-gremlin 3.0.0.M7 

### Java API
1. import the jars from build
2. like gremlin-console. create a configuration and create a new instance of ElasticGraph
```java
BaseConfiguration config = new BaseConfiguration();
/* put configuration properties as you like*/
ElasticGraph g = new ElasticGraph(config);
```
####batch loading:
if you going to load a lot of items to the graph, make use of our batchloading api
just put this properties on the configuration:
```java
  config.addProperty("elasticsearch.batch", true);
  config.addProperty("elasticsearch.refresh", false);
```
and now add vertices and edges , when you want to commit the changes just run
```java
  g.commit();
```
####


