package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SearchQueryOffsetTest {
    @Test
    public void offsetDefaultsZeroAndRoundTrips() {
        SearchQuery<Vertex> q = new SearchQuery<>(Vertex.class, PredicatesHolderFactory.empty(),
                60, Collections.emptySet(), null, null, null);
        assertEquals(0, q.getOffset());
        assertEquals(0, q.getPushedOffset());
        q.setOffset(40);
        q.setPushedOffset(40);
        assertEquals(40, q.getOffset());
        assertEquals(40, q.getPushedOffset());
    }
}
