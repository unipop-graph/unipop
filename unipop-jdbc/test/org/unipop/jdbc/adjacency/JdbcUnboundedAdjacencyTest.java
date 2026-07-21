package org.unipop.jdbc.adjacency;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unbounded g.V().out(L)/in(L)/both(L) is rewritten to g.E().hasLabel(L).inV()/outV()/bothV(),
 * eliminating the source-vertex scan while preserving results (including the trailing has() push-down).
 * Gated by the unipop.unboundedAdjacencyScan flag. Uses the joinpushdown schema (jp_host/jp_person/jp_owns).
 */
public class JdbcUnboundedAdjacencyTest {

    private static GraphTraversalSource g;

    private static void createData() throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jp_host");
            s.execute("DROP TABLE IF EXISTS jp_person");
            s.execute("DROP TABLE IF EXISTS jp_owns");
            s.execute("CREATE TABLE jp_host   (id varchar(100) primary key, status varchar(20), name varchar(50), rack varchar(20))");
            s.execute("CREATE TABLE jp_person (id varchar(100) primary key, status varchar(20), name varchar(50))");
            s.execute("CREATE TABLE jp_owns   (id varchar(100) primary key, src_id varchar(100), src_label varchar(20), dst_id varchar(100), dst_label varchar(20))");
            s.execute("INSERT INTO jp_host VALUES ('root','open','root','r0'),('h1','open','alpha','r1'),('h2','open','bravo','r2'),('h3','closed','charlie','r3')");
            s.execute("INSERT INTO jp_person VALUES ('p1','open','delta'),('p2','open','echo')");
            s.execute("INSERT INTO jp_owns VALUES " +
                    "('o1','root','host','h1','host'),('o2','root','host','h2','host'),('o3','root','host','h3','host')," +
                    "('o4','root','host','p1','person'),('o5','root','host','p2','person'),('o6','h1','host','p1','person')");
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        createData();
        String dir = new File(JdbcUnboundedAdjacencyTest.class.getResource("/configuration/joinpushdown/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "joinpushdown");
        conf.setProperty("providers", dir);
        conf.setProperty("unipop.unboundedAdjacencyScan", true);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Before
    public void clearTiming() {
        TimingExecuterListener.timing.clear();
    }

    private static boolean ranAgainst(String table) {
        return TimingExecuterListener.timing.keySet().stream()
                .anyMatch(sql -> sql.toLowerCase().contains("from " + table.toLowerCase()));
    }

    // --- Correctness (results identical to native semantics) ---

    @Test
    public void outAllNeighbours() {
        // 6 owns edges -> 6 in-vertices (h1,h2,h3,p1,p2,p1)
        assertEquals(6L, (long) g.V().out("owns").count().next());
    }

    @Test
    public void outHasFiltersTarget() {
        // hosts with status open reachable via owns: h1,h2 (h3 closed; persons excluded)
        assertEquals(2L, (long) g.V().out("owns").hasLabel("host").has("status", "open").count().next());
        assertEquals(3L, (long) g.V().out("owns").hasLabel("host").count().next());
    }

    @Test
    public void inNeighbours() {
        // out-vertices of all owns edges: root x5, h1 x1 = 6 (all hosts)
        assertEquals(6L, (long) g.V().in("owns").count().next());
        assertEquals(6L, (long) g.V().in("owns").hasLabel("host").count().next());
    }

    @Test
    public void bothNeighboursNoDoubleCount() {
        // 6 edges x 2 endpoints = 12; person endpoints = p1,p2,p1 = 3
        assertEquals(12L, (long) g.V().both("owns").count().next());
        assertEquals(3L, (long) g.V().both("owns").hasLabel("person").count().next());
    }

    // --- The optimization: source-vertex scan eliminated ---

    @Test
    public void eliminatesSourceScan() {
        assertEquals(2L, (long) g.V().out("owns").hasLabel("host").has("status", "open").count().next());
        assertTrue("edge table must be scanned as the start (g.E())", ranAgainst("jp_owns"));
        assertFalse("person source table must NOT be scanned once g.V() is eliminated", ranAgainst("jp_person"));
    }

    // --- Guards: must fall back (keep source) when source identity is used ---

    @Test
    public void pathKeepsSource() {
        // g.V().out().path() = [sourceV, targetV]; rewrite would drop sourceV, so it must fall back.
        List<Path> paths = g.V().out("owns").path().toList();
        assertEquals(6, paths.size());
        paths.forEach(p -> assertEquals(2, p.size()));
    }

    @Test
    public void labeledSourceKeptForSelect() {
        // select('a') returns the SOURCE vertices; rewrite must fall back so 'a' still resolves.
        List<Vertex> sources = g.V().as("a").out("owns").<Vertex>select("a").toList();
        assertEquals(6, sources.size());
    }

    @Test
    public void boundedSourceNotRewritten() {
        // g.V("root") is bounded; must keep normal semantics (root owns 5: h1,h2,h3,p1,p2) not scan all edges.
        assertEquals(5L, (long) g.V("root").out("owns").count().next());
    }

    // --- Flag gate (control): flag off => old path, source scanned ---

    @Test
    public void flagOffScansSource() throws Exception {
        String dir = new File(getClass().getResource("/configuration/joinpushdown/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "joinpushdown");
        conf.setProperty("providers", dir);
        // no unipop.unboundedAdjacencyScan -> defaults off
        try (Graph off = UniGraph.open(conf)) {
            GraphTraversalSource gOff = off.traversal();
            TimingExecuterListener.timing.clear();
            assertEquals(2L, (long) gOff.V().out("owns").hasLabel("host").has("status", "open").count().next());
            assertTrue("flag off => g.V() enumerates all vertices, incl. jp_person", ranAgainst("jp_person"));
        }
    }
}
