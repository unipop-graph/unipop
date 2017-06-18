package org.unipop.rest.test;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import test.RestGraphProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new RestGraphProvider());
    }

    @Test
    @LoadGraphWith
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1_2_3_4X_name() {
        final Object vLop = convertToVertexId("lop");
        g.V(vLop).drop().iterate();
        final Traversal<Vertex, String> traversal = g.V("1", "2", "3", "4").values("name");
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "josh"), traversal);
        assertFalse(traversal.hasNext());
    }

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertFalse(traversal.hasNext());
        if (expectedResults.size() != results.size()) {
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                assertThat("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).filter(e -> internalCheckMap((Map) e, (Map) t)).findAny().isPresent(), is(true));
            } else {
                assertThat("Checking result existence: " + t, expectedResults.contains(t), is(true));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1l));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1l));
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
        assertThat(traversal.hasNext(), is(false));
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

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V("1", "2", "3", "4").values("name");
        check(t);
    }

    private void check(Traversal traversal) {
//        traversal.profile();
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        int count = 0;
        while (traversal.hasNext()) {
            System.out.println(traversal.next());
            count++;
        }
        System.out.println(count);
    }
}
