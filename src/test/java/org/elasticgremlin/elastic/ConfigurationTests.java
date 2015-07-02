package org.elasticgremlin.elastic;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.ElasticClientFactory;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ConfigurationTests {

    @Test
    public void upsertConfiguration() throws  InstantiationException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph10");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.NODE);
        config.addProperty("elasticsearch.upsert", true);
        ElasticGraph graph = new ElasticGraph(config);
        graph.getQueryHandler().clearAllData();

        graph.addVertex(T.id, "1", "field", "a", "field2", "c");
        graph.addVertex(T.id, "1", "field", "b");

        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V("1");
        Vertex vertex = traversal.next();
        assertEquals("b", vertex.property("field").value());
        assertEquals("c", vertex.property("field2").value());
        assertFalse(traversal.hasNext());
    }
}
