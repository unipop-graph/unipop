package org.unipop.elastic.misc;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic.controllermanagers.AggsControllerManager;
import org.unipop.elastic.controllermanagers.TemplateControllerManager;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 10/02/16.
 */
public class TemplateTests {

    private UniGraph graph;
    private GraphTraversalSource g;

    public TemplateTests() throws InstantiationException {
        BaseConfiguration conf = new BaseConfiguration();
        conf.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
        conf.addProperty("elasticsearch.cluster.name", "elasticsearch");
        conf.addProperty("elasticsearch.cluster.address", "127.0.0.1:9300");
        conf.addProperty("controllerManagerFactory", (ControllerManagerFactory) () -> new AggsControllerManager());
        graph = new UniGraph(conf);
        g = graph.traversal();
    }

    @Test
    public void test() {
        g.V().hasLabel("person","software").forEachRemaining(System.out::println);
    }
}
