package org.unipop.elastic.tests;

import org.apache.tinkerpop.gremlin.*;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.computer;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class TemporaryTests extends AbstractGremlinProcessTest {

    public TemporaryTests() throws Exception {
        GraphManager.setTraversalEngineType(TraversalEngine.Type.STANDARD);
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldAddStartsProperly() {
        final Traversal<Object, Vertex> traversal = out().out();
        printTraversal(traversal);
        assertFalse(traversal.hasNext());

        GraphTraversal<Vertex, Vertex> vertices = g.V();
        Step firstStep = traversal.asAdmin().getSteps().get(0);
        Iterator iterator = traversal.asAdmin().getTraverserGenerator().generateIterator(vertices, firstStep, 1l);
        traversal.asAdmin().addStarts(iterator);
        assertTrue(traversal.hasNext());
        assertEquals(2, IteratorUtils.count(traversal));

        traversal.asAdmin().addStarts(traversal.asAdmin().getTraverserGenerator().generateIterator(g.V(), traversal.asAdmin().getSteps().get(0), 1l));
        traversal.asAdmin().addStarts(traversal.asAdmin().getTraverserGenerator().generateIterator(g.V(), traversal.asAdmin().getSteps().get(0), 1l));
        assertEquals(4, IteratorUtils.count(traversal));
        assertFalse(traversal.hasNext());
    }

    @Test
    public void nullProperty() {
        graph.addVertex("abc", null);
    }

    private void printTraversal(Traversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);
    }

    private void printResults(Traversal traversal) {
        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);
        int count = 0;
        while(traversal.hasNext()) {
            System.out.println(traversal.next());
            count ++;
        }
        System.out.println(count);
    }
}
