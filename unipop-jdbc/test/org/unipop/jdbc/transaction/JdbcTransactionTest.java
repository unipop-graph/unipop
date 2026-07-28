package org.unipop.jdbc.transaction;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A vertex drop cascades to its incident edges; this asserts the cascade is all-or-nothing.
 *
 * The mid-cascade failure is injected with a BEFORE DELETE trigger on the vertex table, which
 * raises only after the edge deletes have already been issued. That is the same shape as the
 * read-only-VIEW vertex source the consumer has: the incident-edge deletes succeed, the vertex
 * delete cannot, and the whole cascade must unwind.
 */
public class JdbcTransactionTest {

    private static GraphTraversalSource g;
    private static UniGraph graph;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        String dir = new File(JdbcTransactionTest.class.getResource("/configuration/transactions/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "transactions");
        conf.setProperty("providers", dir);
        graph = UniGraph.open(conf);
        g = graph.traversal();
    }

    @Before
    public void reset() throws Exception {
        exec("DROP TABLE IF EXISTS t_node",
                "DROP TABLE IF EXISTS t_link",
                "DROP FUNCTION IF EXISTS t_node_block_fn() CASCADE",
                "CREATE TABLE t_node (id varchar(100) primary key, name varchar(50))",
                "CREATE TABLE t_link (id varchar(100) primary key, src_id varchar(100), dst_id varchar(100))",
                // n1 is the delete target: one out-edge, one in-edge, plus an unrelated edge that must survive.
                "INSERT INTO t_node VALUES ('n1','a'),('n2','b'),('n3','c')",
                "INSERT INTO t_link VALUES ('e1','n1','n2'),('e2','n3','n1'),('e3','n2','n3')");
        if (graph.tx().isOpen()) graph.tx().rollback();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Test
    public void supportsTransactionsAndTxIsUsable() {
        assertTrue(graph.features().graph().supportsTransactions());
        assertFalse("threaded transactions are deliberately not supported",
                graph.features().graph().supportsThreadedTransactions());
        graph.tx().rollback();
    }

    @Test
    public void dropVertexCascadesToIncidentEdges() {
        g.V("n1").drop().iterate();
        graph.tx().commit();

        assertEquals(List.of("n2", "n3"), ids("SELECT id FROM t_node ORDER BY id"));
        assertEquals("only the edge untouched by n1 survives", List.of("e3"), ids("SELECT id FROM t_link ORDER BY id"));
    }

    /**
     * The point of the whole change: a failure after the incident edges are deleted but before the
     * vertex delete lands must leave the graph exactly as it was.
     */
    @Test
    public void midCascadeFailureLeavesGraphUnchanged() throws Exception {
        exec("CREATE FUNCTION t_node_block_fn() RETURNS trigger AS $$ BEGIN "
                        + "RAISE EXCEPTION 'injected mid-cascade failure'; END; $$ LANGUAGE plpgsql",
                "CREATE TRIGGER t_node_block BEFORE DELETE ON t_node "
                        + "FOR EACH ROW EXECUTE FUNCTION t_node_block_fn()");

        List<String> nodesBefore = ids("SELECT id FROM t_node ORDER BY id");
        List<String> linksBefore = ids("SELECT id FROM t_link ORDER BY id");

        try {
            g.V("n1").drop().iterate();
            graph.tx().commit();
            fail("the vertex delete is blocked by the trigger - drop must not succeed");
        } catch (Exception expected) {
            // this is what gremlin-server's TraversalOpProcessor.onError does
            graph.tx().rollback();
        }

        assertEquals("vertices must be untouched", nodesBefore, ids("SELECT id FROM t_node ORDER BY id"));
        assertEquals("incident edges must be restored", linksBefore, ids("SELECT id FROM t_link ORDER BY id"));
    }

    @Test
    public void rollbackDiscardsBufferedWrites() {
        g.addV("node").property(T.id, "n9").property("name", "z").next();
        graph.tx().rollback();

        assertFalse("rolled-back insert must not be visible", ids("SELECT id FROM t_node ORDER BY id").contains("n9"));
    }

    @Test
    public void commitFlushesBufferedWrites() {
        g.addV("node").property(T.id, "n9").property("name", "z").next();
        graph.tx().commit();

        assertTrue("committed insert must be visible", ids("SELECT id FROM t_node ORDER BY id").contains("n9"));
    }

    /**
     * The shape that hangs JdbcStructureSuite: IoTest's self-looping-edge round-trip, whose commit now
     * flushes a batch because GraphSONReader.readGraph calls tx().commit() once the transactions feature
     * is on. Whatever the batch does, it must not wedge.
     */
    @Test(timeout = 60_000)
    public void selfLoopingEdgeCommitsWithoutHanging() {
        Vertex v = g.addV("node").property(T.id, "s1").property("name", "loop").next();
        v.addEdge("link", v, T.id, "se1");
        graph.tx().commit();

        assertTrue(ids("SELECT id FROM t_node ORDER BY id").contains("s1"));
        assertEquals(List.of("s1"), ids("SELECT src_id FROM t_link WHERE id = 'se1'"));
        assertEquals(List.of("s1"), ids("SELECT dst_id FROM t_link WHERE id = 'se1'"));
    }

    /**
     * A statement that genuinely fails inside a transaction must surface as an error promptly. The buffer
     * is flushed as a single jOOQ batch on commit, and once one statement in a batch aborts the
     * transaction every later statement in it fails too — the driver's handling of that must not wedge.
     */
    @Test(timeout = 60_000)
    public void failingStatementInsideTransactionDoesNotWedgeCommit() throws Exception {
        exec("CREATE FUNCTION t_node_block_fn() RETURNS trigger AS $$ BEGIN "
                        + "RAISE EXCEPTION 'injected insert failure'; END; $$ LANGUAGE plpgsql",
                "CREATE TRIGGER t_node_block BEFORE INSERT ON t_node "
                        + "FOR EACH ROW EXECUTE FUNCTION t_node_block_fn()");

        for (int i = 0; i < 50; i++) g.addV("node").property(T.id, "x" + i).property("name", "v").next();
        try {
            graph.tx().commit();
            fail("every insert is blocked by the trigger - commit must not succeed");
        } catch (Exception expected) {
            graph.tx().rollback();
        }
        assertEquals("nothing may be added", List.of("n1", "n2", "n3"), ids("SELECT id FROM t_node ORDER BY id"));
    }

    private static void exec(String... sql) throws Exception {
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            for (String q : sql) s.execute(q);
        }
    }

    /** Reads through a connection of its own, so it sees only committed state. */
    private static List<String> ids(String sql) {
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        } catch (Exception e) {
            throw new IllegalStateException(sql, e);
        }
    }
}
