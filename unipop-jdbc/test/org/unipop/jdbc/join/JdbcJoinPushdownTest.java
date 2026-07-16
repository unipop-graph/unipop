package org.unipop.jdbc.join;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcJoinPushdownTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jp_host");
            s.execute("DROP TABLE IF EXISTS jp_person");
            s.execute("DROP TABLE IF EXISTS jp_owns");
            s.execute("CREATE TABLE jp_host   (id varchar(100) primary key, status varchar(20), name varchar(50))");
            s.execute("CREATE TABLE jp_person (id varchar(100) primary key, status varchar(20), name varchar(50))");
            s.execute("CREATE TABLE jp_owns   (id varchar(100) primary key, src_id varchar(100), dst_id varchar(100))");
            // root (host) owns 3 hosts + 2 persons; h1 additionally owns p1 (for both() tests).
            s.execute("INSERT INTO jp_host VALUES ('root','open','root'),('h1','open','alpha'),('h2','open','bravo'),('h3','closed','charlie')");
            s.execute("INSERT INTO jp_person VALUES ('p1','open','delta'),('p2','open','echo')");
            s.execute("INSERT INTO jp_owns VALUES " +
                    "('o1','root','h1'),('o2','root','h2'),('o3','root','h3'),('o4','root','p1'),('o5','root','p2'),('o6','h1','p1')");
        }
        String dir = new File(JdbcJoinPushdownTest.class.getResource("/configuration/joinpushdown/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "joinpushdown");
        conf.setProperty("providers", dir);
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

    /** True if any executed query joined jp_host (i.e. the join path ran on the host table). */
    private static boolean joinedHost() {
        return TimingExecuterListener.timing.keySet().stream()
                .anyMatch(sql -> { String q = sql.toLowerCase(); return q.contains("join") && q.contains("jp_host"); });
    }

    /**
     * True if any executed query is a deferred vertex-property fetch (the pre-join fallback hydration
     * path) against a vertex table. Excludes the id-only "select id from ... where id in (...)" shape
     * that g.V(id) always runs to resolve the traversal's start vertex across every vertex table --
     * that lookup is orthogonal to whether the OUT-hop's target got hydrated via JOIN or a deferred
     * property re-fetch, and would otherwise false-positive this check on every traversal.
     */
    private static boolean ranDeferredVertexFetch() {
        return TimingExecuterListener.timing.keySet().stream()
                .anyMatch(sql -> { String q = sql.toLowerCase().replaceAll("\\s+", " ").trim();
                    return !q.contains("join") && (q.contains("from jp_host") || q.contains("from jp_person"))
                            && q.contains("where") && q.contains("id") && !q.startsWith("select id from"); });
    }

    @Test
    public void outHasLabelHasLimitPushesJoin() {
        assertEquals(2L, (long) g.V("root").out("owns").hasLabel("host").has("status", "open").limit(10).count().next());
        assertTrue("expected a JOIN on jp_host", joinedHost());
    }

    @Test
    public void joinPathSkipsDeferredVertexFetch() {
        long n = g.V("root").out("owns").hasLabel("host").has("status", "open").limit(10)
                .values("name").count().next();
        assertEquals(2L, n);
        assertTrue("expected a JOIN on jp_host", joinedHost());
        assertTrue("join path must not run a separate by-id vertex fetch", !ranDeferredVertexFetch());
    }
}
