package org.unipop.jdbc.range;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** A range pushed to SQL must return the same rows, in the same order, as the range applied in memory. */
public class JdbcRangeOrderTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ro_item");
            s.execute("CREATE TABLE ro_item (id varchar(100) primary key, name varchar(50))");
            // Row order c,a,e,b,d differs from both id order and the hash order of the ids (a,b,c,d,e).
            s.execute("INSERT INTO ro_item VALUES ('c','n'),('a','n'),('e','n'),('b','n'),('d','n')");
        }
        String dir = new File(JdbcRangeOrderTest.class.getResource("/configuration/rangeorder/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "rangeorder");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception { if (g != null) g.close(); }

    @Test
    public void orderByIdSortsBeforeTheRange() {
        // by(T.id) is not pushed to SQL, so the range must not be pushed below it either.
        assertEquals(Arrays.asList("b", "c"), g.V().hasLabel("item").order().by(T.id).range(1, 3).id().toList());
        assertEquals(Arrays.asList("d", "c"), g.V().hasLabel("item").order().by(T.id, org.apache.tinkerpop.gremlin.process.traversal.Order.desc).range(1, 3).id().toList());
    }

    @Test
    public void pagesCutShortEnumerateEveryRowOnce() {
        // A caller fetches 4, keeps the first 2 and resumes at offset + 2 (a page cut by an output cap).
        List<Object> seen = new ArrayList<>();
        for (int offset = 0; offset < 5; offset += 2) {
            List<Map<Object, Object>> page = g.V().hasLabel("item").elementMap().range(offset, offset + 4).toList();
            page.stream().limit(2).forEach(row -> seen.add(row.get(T.id)));
        }
        assertEquals(5, seen.size());
        assertEquals(5, new HashSet<>(seen).size());
    }
}
