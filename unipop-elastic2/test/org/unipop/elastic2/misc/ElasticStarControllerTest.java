package org.unipop.elastic2.misc;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic2.controllermanagers.ElasticStarControllerManager;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.structure.UniGraph;

public class ElasticStarControllerTest {
    UniGraph graph;

    public ElasticStarControllerTest() throws InstantiationException {
        BaseConfiguration conf = new BaseConfiguration();
        conf.addProperty("controllerManager", ElasticStarControllerManager.class.getCanonicalName());
        conf.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
        graph = new UniGraph(conf);
    }

    @org.junit.Test
    public void test() throws Exception {
        Vertex marko = graph.addVertex(T.label,"person", T.id, "1", "name", "marko", "age", 29);

        Vertex josh = graph.addVertex(T.label,"person", T.id, "4", "name", "josh", "age", 32);

        Vertex vadas = graph.addVertex(T.label, "person", T.id, "2", "name" , "vadas", "age" ,27);

        Vertex peter = graph.addVertex(T.label, "person", T.id, "6", "name", "peter", "age", 35);

        Vertex lop = graph.addVertex(T.label,"software", T.id, "3", "name", "lop", "lang", "java");

        Vertex ripple = graph.addVertex(T.label, "software", T.id, "5", "name","ripple", "lang","java");

        marko.addEdge("created", lop, "weight", 0.4d, T.id,"9");
        marko.addEdge("knows", vadas, "weight", 0.5d, T.id, "7");
        marko.addEdge("knows", josh, "weight", 1.0d, T.id,"8");

        josh.addEdge("created", lop, "weight", 0.4d,T.id,"11");
        josh.addEdge("created", ripple, "weight", 1.0d,T.id,"10");

        peter.addEdge("created", lop, "weight", 0.2d,T.id,"12");
        graph.traversal().E().sample(2).by("weight")
                .forEachRemaining(System.out::println);
    }
}
