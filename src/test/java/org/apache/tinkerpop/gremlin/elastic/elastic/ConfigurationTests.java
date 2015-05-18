package org.apache.tinkerpop.gremlin.elastic.elastic;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ConfigurationTests {

    @Test
    public void upsertConfiguration() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", "test");
        config.addProperty("elasticsearch.index.name", "graph10");
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE);
        config.addProperty("elasticsearch.upsert", true);
        ElasticGraph graph = new ElasticGraph(config);
        graph.elasticService.clearAllData();

        graph.addVertex(T.id, "1", "field", "a", "field2", "c");
        graph.addVertex(T.id, "1", "field", "b");

        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V("1");
        Vertex vertex = traversal.next();
        assertEquals("b", vertex.property("field").value());
        assertEquals("c", vertex.property("field2").value());
        assertFalse(traversal.hasNext());
    }
}
