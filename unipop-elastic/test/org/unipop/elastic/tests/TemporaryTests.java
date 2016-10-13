package org.unipop.elastic.tests;

import org.apache.tinkerpop.gremlin.*;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.ElasticGraphProvider;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static org.junit.Assert.assertThat;

public class TemporaryTests extends AbstractGremlinTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinTest.class);

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX() {
        final Traversal<Vertex, Map<String, Map<String, Long>>> traversal = g.V().hasLabel("song").<String, Map<String, Long>>group().by("name").by(__.properties().groupCount().by(T.label));
        printTraversalForm(traversal);
        final Map<String, Map<String, Long>> map = traversal.next();
        assertEquals(584, map.size());
        for (final Map.Entry<String, Map<String, Long>> entry : map.entrySet()) {
            assertEquals(entry.getKey().toUpperCase(), entry.getKey());
            final Map<String, Long> countMap = entry.getValue();
            assertEquals(3, countMap.size());
            assertEquals(1l, countMap.get("name").longValue());
            assertEquals(1l, countMap.get("songType").longValue());
            assertEquals(1l, countMap.get("performances").longValue());
        }
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX() {
        final Traversal<Vertex, Map<String, Collection<Double>>> traversal =
                g.V().<String, Collection<Double>>group().by(T.label).by(bothE().values("weight").sample(2).fold());
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Collection<Double>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(2, map.get("software").size());
        assertEquals(2, map.get("person").size());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V().local(outE().sample(1).by("weight"));

        check(t);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test_a() {
        final Traversal<Object, Vertex> traversal = out().out();
        assertFalse(traversal.hasNext());
        traversal.asAdmin().addStarts(traversal.asAdmin().getTraverserGenerator().generateIterator(g.V(), traversal.asAdmin().getSteps().get(0), 1l));
        assertTrue(traversal.hasNext());
        assertEquals(2, IteratorUtils.count(traversal));
    }

    @Test
    public void nullProperty() {
        graph.addVertex("abc", null);
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
