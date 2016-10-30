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

    public TemporaryTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void test() {
        Traversal t = g.V().hasLabel("person").has("age", P.not(P.lte(10).and(P.not(P.between(11, 20)))).and(P.lt(29).or(P.eq(35)))).values("name");

        check(t);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX() {
        final Traversal<Vertex, Map<String, Map<String, Number>>> traversal = g.V().out("followedBy").<String, Map<String, Number>>group().by("songType").by(__.bothE().group().by(T.label).by(__.values("weight").sum()));
        final Map<String, Map<String, Number>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(3, map.size());
        assertTrue(map.containsKey(""));
        assertTrue(map.containsKey("original"));
        assertTrue(map.containsKey("cover"));
        //
        Map<String, Number> subMap = map.get("");
        assertEquals(1, subMap.size());
        assertEquals(179350, subMap.get("followedBy").intValue());
        //
        subMap = map.get("original");
        assertEquals(3, subMap.size());
        assertEquals(2185613, subMap.get("followedBy").intValue());
        assertEquals(0, subMap.get("writtenBy").intValue());
        assertEquals(0, subMap.get("sungBy").intValue());
        //
        subMap = map.get("cover");
        assertEquals(3, subMap.size());
        assertEquals(777982, subMap.get("followedBy").intValue());
        assertEquals(0, subMap.get("writtenBy").intValue());
        assertEquals(0, subMap.get("sungBy").intValue());
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
