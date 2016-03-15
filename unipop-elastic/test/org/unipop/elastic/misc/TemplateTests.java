package org.unipop.elastic.misc;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;
import org.unipop.elastic.controllerprovider.ImdbControllerProvider;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.process.strategyregistrar.StandardStrategyRegistrar;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 10/02/16.
 */
public class TemplateTests {

    private GraphTraversalSource g;

    public TemplateTests() throws Exception {
        BaseConfiguration conf = new BaseConfiguration();
        conf.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
        conf.addProperty("elasticsearch.cluster.name", "elasticsearch");
        conf.addProperty("elasticsearch.cluster.address", "127.0.0.1:9300");
        conf.addProperty("controllerProvider", new ImdbControllerProvider());
        UniGraph graph = new UniGraph(conf);
        g = graph.traversal();
    }

    @Test
    public void test() {
        g.V().hasLabel("genre").valueMap().forEachRemaining(System.out::println);
    }
}
