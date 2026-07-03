package org.unipop.jdbc.uuid;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises a property backed by a PostgreSQL native uuid column (write + query + read + update),
 * with both java.util.UUID and String values, against embedded PostgreSQL.
 */
public class JdbcUuidTest {

    private static GraphTraversalSource g;
    private static final UUID U1 = UUID.fromString("f47ac10b-58cc-4372-a567-0f02b2f3d479");
    private static final UUID U2 = UUID.fromString("9f1a7b2c-3d4e-4f60-8181-92a3b4c5d6e7");

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS uuids");
            s.execute("CREATE TABLE uuids (id varchar(100) primary key, label varchar(100), name varchar(100), ref uuid)");
        }
        String dir = new File(JdbcUuidTest.class.getResource("/configuration/uuid/uuids.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "uuidtest");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @Before
    public void clean() throws Exception {
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("TRUNCATE TABLE uuids");
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Test
    public void writeUuidValueReadBackAsUuidAndQuery() {
        g.addV("thing").property(T.id, "1").property("name", "a").property("ref", U1).next();
        Object read = g.V("1").values("ref").next();
        assertEquals(U1, read);
        assertTrue("values(ref) must be a java.util.UUID", read instanceof UUID);
        assertEquals(1L, (long) g.V().has("ref", U1).count().next());
        assertEquals(1L, (long) g.V().has("ref", P.within(U1)).count().next());
    }

    @Test
    public void writeUuidStringReadBackAsUuidAndQueryByString() {
        g.addV("thing").property(T.id, "2").property("name", "b").property("ref", U2.toString()).next();
        Object read = g.V("2").values("ref").next();
        assertEquals(U2, read);
        assertTrue(read instanceof UUID);
        assertEquals(1L, (long) g.V().has("ref", U2.toString()).count().next());
    }

    @Test
    public void updateUuid() {
        g.addV("thing").property(T.id, "3").property("name", "c").property("ref", U1).next();
        g.V("3").property("ref", U2).next();
        assertEquals(U2, g.V("3").values("ref").next());
        assertEquals(0L, (long) g.V().has("ref", U1).count().next());
        assertEquals(1L, (long) g.V().has("ref", U2).count().next());
    }
}
