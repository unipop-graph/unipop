package org.unipop.elastic2.misc;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic2.controllermanagers.ImdbControllerManager;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.process.strategy.SimplifiedStrategyRegistrar;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 10/02/16.
 */
public class TemplateTests {

    private GraphTraversalSource g;

    public TemplateTests() throws InstantiationException {
        BaseConfiguration conf = new BaseConfiguration();
        conf.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
        conf.addProperty("elasticsearch.cluster.name", "elasticsearch");
        conf.addProperty("elasticsearch.cluster.address", "127.0.0.1:9300");
        conf.addProperty("controllerManagerFactory", (ControllerManagerFactory) ImdbControllerManager::new);
        conf.addProperty("strategyRegistrarClass", SimplifiedStrategyRegistrar.class.getCanonicalName());
        UniGraph graph = new UniGraph(conf);
        g = graph.traversal();
    }

    @Test
    public void test() {
        g.V().outE().as("edge").otherV().as("vertex").select("edge", "vertex").by(__.valueMap(true)).forEachRemaining(System.out::println);
    }
}
