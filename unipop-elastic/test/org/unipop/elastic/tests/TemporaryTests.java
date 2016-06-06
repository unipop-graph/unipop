package org.unipop.elastic.tests;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.elastic.ElasticGraphProvider;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.computer;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class TemporaryTests extends AbstractGremlinTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinTest.class);

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V().repeat(__.union(__.out("knows").group("a").by("age"), __.out("created").group("b").by("name").by(count())).group("a").by("name")).times(2).cap("a", "b");

        check(t);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = g.V().match(
                as("a").out("created").has("name", "lop").as("b"),
                as("b").in("created").has("age", 29).as("c"))
                .where(__.<Vertex>as("c").repeat(out()).times(2)).select("a", "b", "c");
        printTraversalForm(traversal);
        checkResults(makeMapList(3,
                "a", convertToVertex(graph, "marko"), "b", convertToVertex(graph, "lop"), "c", convertToVertex(graph, "marko"),
                "a", convertToVertex(graph, "josh"), "b", convertToVertex(graph, "lop"), "c", convertToVertex(graph, "marko"),
                "a", convertToVertex(graph, "peter"), "b", convertToVertex(graph, "lop"), "c", convertToVertex(graph, "marko")), traversal);
    }

    public <A, B> List<Map<A, B>> makeMapList(final int size, final Object... keyValues) {
        final List<Map<A, B>> mapList = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i = i + (2 * size)) {
            final Map<A, B> map = new HashMap<>();
            for (int j = 0; j < (2 * size); j = j + 2) {
                map.put((A) keyValues[i + j], (B) keyValues[i + j + 1]);
            }
            mapList.add(map);
        }
        return mapList;
    }

    private static <A, B> boolean internalCheckMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() != actualList.size()) {
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

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertFalse(traversal.hasNext());
        if (expectedResults.size() != results.size()) {
            logger.error("Expected results: " + expectedResults);
            logger.error("Actual results:   " + results);
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

    @Test
    public void nullProperty() {
        graph.addVertex("abc", null);
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
