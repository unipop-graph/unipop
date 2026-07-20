package org.unipop.jdbc.multihop;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 2-hop pushdown on the joinpushdown fixture: root -owns-> h1 -owns-> p1.
 */
public class JdbcMultiHopPushdownTest {

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
            s.execute("CREATE TABLE jp_host   (id varchar(100) primary key, status varchar(20), name varchar(50), rack varchar(20))");
            s.execute("CREATE TABLE jp_person (id varchar(100) primary key, status varchar(20), name varchar(50))");
            s.execute("CREATE TABLE jp_owns   (id varchar(100) primary key, src_id varchar(100), src_label varchar(20), dst_id varchar(100), dst_label varchar(20))");
            s.execute("INSERT INTO jp_host VALUES ('root','open','root','r0'),('h1','open','alpha','r1'),('h2','open','bravo','r2'),('h3','closed','charlie','r3')");
            s.execute("INSERT INTO jp_person VALUES ('p1','open','delta'),('p2','open','echo')");
            s.execute("INSERT INTO jp_owns VALUES " +
                    "('o1','root','host','h1','host'),('o2','root','host','h2','host'),('o3','root','host','h3','host')," +
                    "('o4','root','host','p1','person'),('o5','root','host','p2','person'),('o6','h1','host','p1','person')");
        }
        String dir = new File(JdbcMultiHopPushdownTest.class.getResource("/configuration/joinpushdown/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "multihop");
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

    private static boolean multiJoinedPerson() {
        return TimingExecuterListener.timing.keySet().stream().anyMatch(sql -> {
            String q = sql.toLowerCase().replaceAll("\\s+", " ");
            // two edge aliases join into person: e0/e1 pattern or two joins of jp_owns
            int owns = countOccurrences(q, "jp_owns");
            return q.contains("join") && owns >= 2 && q.contains("jp_person");
        });
    }

    private static int countOccurrences(String hay, String needle) {
        int c = 0, i = 0;
        while ((i = hay.indexOf(needle, i)) >= 0) {
            c++;
            i += needle.length();
        }
        return c;
    }

    @Test
    public void twoHopOutOutReachesPerson() {
        // root -> h1 -> p1
        assertEquals(1L, (long) g.V("root").out("owns").out("owns").hasLabel("person").count().next());
        assertTrue("expected multi-join involving jp_owns twice and jp_person", multiJoinedPerson());
    }

    @Test
    public void twoHopWithFinalHasAndLimit() {
        assertEquals(1L, (long) g.V("root").out("owns").out("owns")
                .has("status", "open").limit(5).count().next());
        assertTrue(multiJoinedPerson());
    }

    @Test
    public void singleHopStillWorks() {
        assertEquals(2L, (long) g.V("root").out("owns").hasLabel("host").has("status", "open").count().next());
    }

    @Test
    public void twoHopOutThenOutECorrectAndSequentialByDefault() {
        // Edge-final is not multi-joined by default (sequential is faster on open topologies).
        TimingExecuterListener.timing.clear();
        assertEquals(1L, (long) g.V("root").out("owns").outE("owns").count().next());
        assertFalse("unbounded out().outE() must not multi-join by default", multiJoinedOwnsTwice());
    }

    private static boolean multiJoinedOwnsTwice() {
        return TimingExecuterListener.timing.keySet().stream().anyMatch(sql -> {
            String q = sql.toLowerCase().replaceAll("\\s+", " ");
            return q.contains("join") && countOccurrences(q, "jp_owns") >= 2;
        });
    }

    @Test
    public void killSwitchFallsBackButStaysCorrect() {
        // Re-open graph with multi-hop disabled
        String dir;
        try {
            dir = new File(JdbcMultiHopPushdownTest.class.getResource("/configuration/joinpushdown/graph.json").toURI()).getParent();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "multihop-off");
        conf.setProperty("providers", dir);
        conf.setProperty("jdbc.multiHopPushdown", false);
        GraphTraversalSource g2 = null;
        try {
            g2 = UniGraph.open(conf).traversal();
            TimingExecuterListener.timing.clear();
            assertEquals(1L, (long) g2.V("root").out("owns").out("owns").hasLabel("person").count().next());
            assertFalse("kill-switch must not multi-join", multiJoinedPerson());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (g2 != null) {
                try { g2.close(); } catch (Exception ignored) {}
            }
        }
    }
}
