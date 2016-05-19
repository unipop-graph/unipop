package org.unipop.elastic.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.computer;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
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
        Traversal t = graph.traversal().V().has("name", "josh").outE("created").as("e").inV().has("name", "lop");

        check(t);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Edge toDetach = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Vertex outV = toDetach.vertices(Direction.OUT).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(toDetach, true);
        final Edge attached = detachedEdge.attach(Attachable.Method.get(outV));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedEdge);
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
