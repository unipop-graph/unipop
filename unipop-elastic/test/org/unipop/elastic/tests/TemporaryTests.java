package org.unipop.elastic.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;
import org.unipop.test.UnipopGraphProvider;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.computer;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V().out().out().valueMap();

        check(t);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldEvaluateConnectivityPatterns() {
        final Vertex a;
        final Vertex b;
        final Vertex c;
        final Vertex d;
        if (graph.features().vertex().supportsUserSuppliedIds()) {
            a = graph.addVertex(T.id, graphProvider.convertId("1", Vertex.class));
            b = graph.addVertex(T.id, graphProvider.convertId("2", Vertex.class));
            c = graph.addVertex(T.id, graphProvider.convertId("3", Vertex.class));
            d = graph.addVertex(T.id, graphProvider.convertId("4", Vertex.class));
        } else {
            a = graph.addVertex();
            b = graph.addVertex();
            c = graph.addVertex();
            d = graph.addVertex();
        }

        tryCommit(graph, assertVertexEdgeCounts(4, 0));

        final Edge e = a.addEdge(graphProvider.convertLabel("knows"), b);
        final Edge f = b.addEdge(graphProvider.convertLabel("knows"), c);
        final Edge g = c.addEdge(graphProvider.convertLabel("knows"), d);
        final Edge h = d.addEdge(graphProvider.convertLabel("knows"), a);

        tryCommit(graph, assertVertexEdgeCounts(4, 4));

        graph.vertices().forEachRemaining(v -> {
            assertEquals(1l, IteratorUtils.count(v.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(v.edges(Direction.IN)));
        });

        graph.edges().forEachRemaining(x -> {
            assertEquals(graphProvider.convertLabel("knows"), x.label());
        });

        if (graph.features().vertex().supportsUserSuppliedIds()) {
            final Vertex va = graph.vertices(graphProvider.convertId("1", Vertex.class)).next();
            final Vertex vb = graph.vertices(graphProvider.convertId("2", Vertex.class)).next();
            final Vertex vc = graph.vertices(graphProvider.convertId("3", Vertex.class)).next();
            final Vertex vd = graph.vertices(graphProvider.convertId("4", Vertex.class)).next();

            assertEquals(a, va);
            assertEquals(b, vb);
            assertEquals(c, vc);
            assertEquals(d, vd);

            assertEquals(1l, IteratorUtils.count(va.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(va.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vb.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vb.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.OUT)));

            final Edge i = a.addEdge(graphProvider.convertLabel("hates"), b);

            assertEquals(1l, IteratorUtils.count(va.edges(Direction.IN)));
            assertEquals(2l, IteratorUtils.count(va.edges(Direction.OUT)));
            assertEquals(2l, IteratorUtils.count(vb.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vb.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vc.edges(Direction.OUT)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.IN)));
            assertEquals(1l, IteratorUtils.count(vd.edges(Direction.OUT)));

            for (Edge x : IteratorUtils.list(a.edges(Direction.OUT))) {
                assertTrue(x.label().equals(graphProvider.convertLabel("knows")) || x.label().equals(graphProvider.convertLabel("hates")));
            }

            assertEquals(graphProvider.convertLabel("hates"), i.label());
            assertEquals(graphProvider.convertId("2", Vertex.class).toString(), i.inVertex().id().toString());
            assertEquals(graphProvider.convertId("1", Vertex.class).toString(), i.outVertex().id().toString());
        }

        final Set<Object> vertexIds = new HashSet<>();
        vertexIds.add(a.id());
        vertexIds.add(a.id());
        vertexIds.add(b.id());
        vertexIds.add(b.id());
        vertexIds.add(c.id());
        vertexIds.add(d.id());
        vertexIds.add(d.id());
        vertexIds.add(d.id());
        assertEquals(4, vertexIds.size());
    }

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertFalse(traversal.hasNext());
        if(expectedResults.size() != results.size()) {
            System.out.println("Expected results: " + expectedResults);
            System.out.println("Actual results:   " + results);
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                assertTrue("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).filter(e -> internalCheckMap((Map) e, (Map) t)).findAny().isPresent());
            } else {
                assertTrue("Checking result existence: " + t, expectedResults.contains(t));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1l));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1l));
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
        assertFalse(traversal.hasNext());
    }

    private static <A, B> boolean internalCheckMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() > actualList.size()) {
            return false;
        } else if (actualList.size() > expectedList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).getKey().equals(expectedList.get(i).getKey())) {
                return false;
            }
            if (!actualList.get(i).getValue().equals(expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    private void check(Traversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);
        int count = 0;
        while(traversal.hasNext()) {
            System.out.println(traversal.next());
            count ++;
        }
        System.out.println(count);
    }
}
