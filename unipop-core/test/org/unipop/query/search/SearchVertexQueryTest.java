package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.javatuples.Pair;
import org.junit.Test;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SearchVertexQueryTest {

    @Test
    public void oldConstructorHasNoTargetIntent() {
        SearchVertexQuery q = new SearchVertexQuery(Edge.class, Collections.emptyList(), Direction.OUT,
                PredicatesHolderFactory.empty(), 5, Collections.emptySet(), null, null, null);
        assertFalse(q.isHydrateTarget());
        assertEquals(-1, q.getTargetLimit());
        assertTrue(q.getTargetPredicates().isEmpty());
    }

    @Test
    public void newConstructorCarriesTargetIntent() {
        List<Pair<String, Order>> orders = Collections.singletonList(Pair.with("name", Order.asc));
        SearchVertexQuery q = new SearchVertexQuery(Edge.class, Collections.emptyList(), Direction.OUT,
                PredicatesHolderFactory.empty(), -1, null, null,
                PredicatesHolderFactory.predicate(new org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer(
                        "status", org.apache.tinkerpop.gremlin.process.traversal.P.eq("open"))),
                orders, 50, true);
        assertTrue(q.isHydrateTarget());
        assertEquals(50, q.getTargetLimit());
        assertEquals(orders, q.getTargetOrders());
        assertTrue(q.getTargetPredicates().notEmpty());
    }
}
