package org.unipop.elastic.tests;

import org.apache.tinkerpop.gremlin.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.ElasticGraphProvider;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static org.junit.Assert.assertThat;

public class TemporaryTests extends AbstractGremlinTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinTest.class);

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldTraversalResetProperly() {
        final Traversal<Object, Vertex> traversal = as("a").out().out().has("name", P.within("ripple", "lop")).as("b");
        if (new Random().nextBoolean()) traversal.asAdmin().reset();
        assertFalse(traversal.hasNext());
        traversal.asAdmin().addStarts(traversal.asAdmin().getTraverserGenerator().generateIterator(g.V(), traversal.asAdmin().getSteps().get(0), 1l));
        assertTrue(traversal.hasNext());
        assertEquals(2, IteratorUtils.count(traversal));

        if (new Random().nextBoolean()) traversal.asAdmin().reset();
        traversal.asAdmin().addStarts(traversal.asAdmin().getTraverserGenerator().generateIterator(g.V(), traversal.asAdmin().getSteps().get(0), 1l));
        assertTrue(traversal.hasNext());
        traversal.next();
        assertTrue(traversal.hasNext());
        traversal.asAdmin().reset();
        assertFalse(traversal.hasNext());

        traversal.asAdmin().addStarts(traversal.asAdmin().getTraverserGenerator().generateIterator(g.V(), traversal.asAdmin().getSteps().get(0), 1l));
        assertEquals(2, IteratorUtils.count(traversal));

        assertFalse(traversal.hasNext());
        if (new Random().nextBoolean()) traversal.asAdmin().reset();
        assertFalse(traversal.hasNext());
    }

    @Test
    public void test() {
        graph.addVertex(T.label, "person", T.id, "1", "name", "marko");
        Traversal t = g.V();
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
