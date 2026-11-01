package org.unipop.jdbc.repeat;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Path searches over {@code repeat(both().simplePath())}, the shape of a shortest-path query.
 */
public class JdbcPathSearchTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ps_node");
            s.execute("DROP TABLE IF EXISTS ps_link");
            s.execute("CREATE TABLE ps_node (id varchar(10) primary key, name varchar(10))");
            s.execute("CREATE TABLE ps_link (id varchar(10) primary key, src_id varchar(10), dst_id varchar(10))");
            s.execute("INSERT INTO ps_node VALUES ('a','a'),('b1','b1'),('b2','b2'),('b3','b3'),"
                    + "('c1','c1'),('c2','c2'),('c3','c3'),('t','t')");
            // Two triangles (b*, c*) bridged pairwise, so a has many simple paths but one 3-hop path to t:
            // a-b1-c1-t.
            s.execute("INSERT INTO ps_link VALUES "
                    + "('l1','a','b1'),('l2','a','b2'),('l3','a','b3'),"
                    + "('l4','b1','b2'),('l5','b2','b3'),('l6','b1','b3'),"
                    + "('l7','b1','c1'),('l8','b2','c2'),('l9','b3','c3'),"
                    + "('l10','c1','c2'),('l11','c2','c3'),('l12','c1','c3'),"
                    + "('l13','c1','t')");
        }
        String dir = new File(JdbcPathSearchTest.class.getResource("/configuration/pathsearch/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "pathsearch");
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

    /** Distinct statements that read ps_link; each hop batch has its own frontier, so its own SQL. */
    private static long linkStatements() {
        return TimingExecuterListener.timing.keySet().stream().filter(sql -> sql.contains("ps_link")).count();
    }

    private static boolean ranJoin() {
        return TimingExecuterListener.timing.keySet().stream().anyMatch(sql -> sql.toLowerCase().contains(" join "));
    }

    private static List<Path> shortestPathToT() {
        return g.V("a").repeat(__.both().simplePath())
                .until(__.hasId("t").or().loops().is(P.gte(6)))
                .hasId("t").path().by(__.id()).limit(1).toList();
    }

    @Test
    public void repeatYieldsItsFirstResultWithoutRunningTheWholeRepeat() {
        AtomicInteger expanded = new AtomicInteger();
        g.V("a").repeat(__.both().simplePath().sideEffect(t -> expanded.incrementAndGet()))
                .emit().times(6).next();
        assertEquals(1, expanded.get());
    }

    @Test
    public void limitStopsWithoutPullingAnotherTraverser() {
        AtomicInteger pulled = new AtomicInteger();
        // A filter, unlike a side effect, keeps EarlyLimitStrategy from moving the limit ahead of it.
        assertEquals(1, g.V("a").both().filter(t -> pulled.incrementAndGet() > 0).limit(1).toList().size());
        assertEquals(1, pulled.get());
    }

    @Test
    public void shortestPathHopsDoNotHydrateVertices() {
        assertEquals(1, shortestPathToT().size());
        assertFalse("hops that only feed ids must not join the vertex table", ranJoin());
    }

    @Test
    public void shortestPathRunsOneStatementPerHop() {
        List<Path> paths = shortestPathToT();
        assertEquals(1, paths.size());
        assertEquals(Arrays.asList("a", "b1", "c1", "t"), paths.get(0).objects());
        assertEquals(3, linkStatements());
    }

    @Test
    public void repeatOutputReturnedToTheCallerIsHydrated() {
        assertEquals(3, g.V("a").repeat(__.both().simplePath()).times(1).toList().size());
        assertTrue("vertices returned to the caller are fetched with their properties", ranJoin());
    }

    @Test
    public void untilReadingAPropertyHydratesTheHop() {
        List<Path> paths = g.V("a").repeat(__.both().simplePath()).until(__.has("name", "b2"))
                .path().by(__.id()).limit(1).toList();
        assertEquals(Arrays.asList("a", "b2"), paths.get(0).objects());
        assertTrue("until(has(name)) reads the hop's properties", ranJoin());
    }
}
