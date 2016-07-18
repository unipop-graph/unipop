package org.unipop;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by sbarzilay on 7/7/16.
 */
public class MultiFieldTests extends AbstractGremlinTest {
    public MultiFieldTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_hasXname_markoX_out() throws Exception {
        GraphTraversal<Vertex, Vertex> t = g.V().has("name", "marko").out();

        List<Vertex> vertices = t.toList();

        assertEquals(3, vertices.size());

        Map<String, Vertex> idToVertex = new HashMap<>();

        vertices.forEach(vertex -> idToVertex.put(vertex.id().toString(), vertex));

        assertTrue(idToVertex.containsKey("4"));
        assertEquals("josh_32", idToVertex.get("4").property("name_age").value().toString());
        assertTrue(idToVertex.containsKey("2"));
        assertEquals("vadas_27", idToVertex.get("2").property("name_age").value().toString());
        assertTrue(idToVertex.containsKey("3"));
        assertFalse(idToVertex.get("3").property("name_age").isPresent());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_out() throws Exception {
        GraphTraversal<Vertex, Vertex> t = g.V().out();

        List<Vertex> vertices = t.toList();

        assertEquals(6, vertices.size());

        Map<String, List<Vertex>> idToVertices = vertices.stream()
                .collect(Collectors.groupingBy(vertex -> vertex.id().toString()));

        assertTrue(idToVertices.containsKey("2"));
        assertEquals(1, idToVertices.get("2").size());
        assertTrue(idToVertices.containsKey("3"));
        assertEquals(3, idToVertices.get("3").size());
        assertTrue(idToVertices.containsKey("4"));
        assertEquals(1, idToVertices.get("4").size());
        assertTrue(idToVertices.containsKey("5"));
        assertEquals(1, idToVertices.get("5").size());

        idToVertices.get("5").forEach(vertex -> assertFalse(vertex.property("name_age").isPresent()));
        idToVertices.get("3").forEach(vertex -> assertFalse(vertex.property("name_age").isPresent()));
        idToVertices.get("4").forEach(vertex -> assertEquals("josh_32", vertex.property("name_age").value().toString()));
        idToVertices.get("2").forEach(vertex -> assertEquals("vadas_27", vertex.property("name_age").value().toString()));
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_hasXname_age_marko_29X() throws Exception {
        GraphTraversal<Vertex, Vertex> t = g.V().has("name_age", "marko_29");
        List<Vertex> vertices = t.toList();
        assertEquals(1, vertices.size());
        assertTrue(vertices.get(0).id().equals("1"));
        List<VertexProperty<Object>> properties = IteratorUtils.<VertexProperty<Object>>asList(vertices.get(0).properties());
        assertEquals(3, properties.size());
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_V_hasXname_age_within_marko_29_vadas_27X_order_byXidX() throws Exception {
        GraphTraversal<Vertex, Vertex> t = g.V().has("name_age", P.within("marko_29", "vadas_27")).order().by(T.id);
        List<Vertex> vertices = t.toList();
        assertEquals(2, vertices.size());
        assertTrue(vertices.get(0).id().equals("1"));
        assertTrue(vertices.get(1).id().equals("2"));
        vertices.forEach(vertex -> {
            List<VertexProperty<Object>> properties = IteratorUtils.<VertexProperty<Object>>asList(vertex.properties());
            assertEquals(3, properties.size());
        });
    }

    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @Test
    public void g_EX7_05X() throws Exception {
        GraphTraversal<Edge, Edge> e = g.E("7_0.5");
        List<Edge> edges = e.toList();
        assertEquals(1, edges.size());
        Edge edge = edges.get(0);
        assertEquals("7_0.5", edge.id().toString());
        assertEquals("0.5", edge.property("weight").value().toString());
    }
}
