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
            s.execute("CREATE TABLE jp_owns   (id varchar(100) primary key, src_id varchar(100), src_label varchar(20), dst_id varchar(100), dst_label varchar(20))");
            // root (host) owns 3 hosts + 2 persons; h1 additionally owns p1 (for both() tests).
            s.execute("INSERT INTO jp_host VALUES ('root','open','root'),('h1','open','alpha'),('h2','open','bravo'),('h3','closed','charlie')");
            s.execute("INSERT INTO jp_person VALUES ('p1','open','delta'),('p2','open','echo')");
            // dst is heterogeneous (host or person), so src/dst labels are stored per-row and the
            // reference vertex schemas below resolve them dynamically (@src_label/@dst_label) rather
            // than a single hardcoded label -- a static label would abort any batch whose real label
            // doesn't match it (e.g. in("owns") from a person-labeled start vertex).
            s.execute("INSERT INTO jp_owns VALUES " +
                    "('o1','root','host','h1','host'),('o2','root','host','h2','host'),('o3','root','host','h3','host')," +
                    "('o4','root','host','p1','person'),('o5','root','host','p2','person'),('o6','h1','host','p1','person')");
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

    /** True if any executed query joined jp_person (i.e. the join path ran on the person table). */
    private static boolean joinedPerson() {
        return TimingExecuterListener.timing.keySet().stream()
                .anyMatch(sql -> { String q = sql.toLowerCase(); return q.contains("join") && q.contains("jp_person"); });
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

    @Test
    public void inDirectionJoinPushes() {
        // p1's in-neighbours via owns: root and h1 (both hosts, both open) -> 2
        assertEquals(2L, (long) g.V("p1").in("owns").hasLabel("host").has("status","open").limit(10).count().next());
        assertTrue("host table joined", joinedHost());
    }

    @Test
    public void bothSingleSource() {
        // h1: in-edge root->h1, out-edge h1->p1  => both = {root, p1} = 2
        assertEquals(2L, (long) g.V("h1").both("owns").limit(10).count().next());
        assertEquals(1L, (long) g.V("h1").both("owns").hasLabel("person").limit(10).count().next()); // only p1
    }

    @Test
    public void bothWholeGraphNoDoubleCount() {
        // 6 owns edges, each contributes 2 both()-traversers (once per direction) => 12, NOT 24.
        assertEquals(12L, (long) g.V().both("owns").count().next());
        // person neighbours only: root->p1, root->p2, h1->p1 => 3
        assertEquals(3L, (long) g.V().both("owns").hasLabel("person").count().next());
        assertTrue("both() with target filter must use the join path", joinedPerson());
    }

    @Test
    public void orderByTargetPropPushesAndTrimsGlobally() {
        // root owns hosts alpha(h1),bravo(h2),charlie(h3); order by name asc, limit 2 => alpha,bravo
        java.util.List<Object> names = g.V("root").out("owns").hasLabel("host")
                .order().by("name").limit(2).values("name").toList();
        assertEquals(java.util.Arrays.asList("alpha","bravo"), names);
        assertTrue("host table joined", joinedHost());
    }

    @Test
    public void hasLabelNarrowsFanOutToOneTable() {
        assertEquals(2L, (long) g.V("root").out("owns").hasLabel("host").has("status", "open").limit(10).count().next());
        assertTrue("host table joined", joinedHost());
        assertTrue("person table must NOT be joined when hasLabel('host') narrows", !joinedPerson());
    }

    @Test
    public void noLabelFansOutOverAllVertexTables() {
        // root owns open hosts h1,h2 (h3 closed) + open persons p1,p2 => 4.
        assertEquals(4L, (long) g.V("root").out("owns").has("status", "open").limit(10).count().next());
        assertTrue(joinedHost());
        assertTrue(joinedPerson());
    }

    @Test
    public void bareLimitDoesNotJoin() {
        assertEquals(5L, (long) g.V("root").out("owns").limit(10).count().next());
        assertTrue("bare limit (no filter/order/props) must not take the join path", !joinedHost() && !joinedPerson());
    }
}
