package org.unipop.jdbc.jsonb;

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

import static org.junit.Assert.assertEquals;

/**
 * Exercises two prefix-addressed JSONB catch-all columns on embedded PostgreSQL: properties keyed
 * {@code <column>.<path>} route to the right column, read back prefixed, and don't collide on a
 * shared sub-key.
 */
public class JdbcMultiJsonbTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS mdocs");
            s.execute("CREATE TABLE mdocs (id varchar(100) primary key, label varchar(100), name varchar(100), data jsonb, meta jsonb)");
        }
        String dir = new File(JdbcMultiJsonbTest.class.getResource("/configuration/multijsonb/docs.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "multijsonbtest");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Test
    public void shouldRouteQueryAndReadTwoJsonbColumns() {
        g.addV("doc").property(T.id, "1").property("name", "a")
                .property("data.status", "active")
                .property("meta.addr.city", "NYC")
                .property("data.k", "fromData")
                .property("meta.k", "fromMeta")
                .next();

        // each column queried independently
        assertEquals(1L, (long) g.V().has("data.status", "active").count().next());
        // nested path in the second column
        assertEquals(1L, (long) g.V().has("meta.addr.city", "NYC").count().next());
        // read-back is prefixed
        assertEquals("active", g.V("1").values("data.status").next());
        // no collision on a shared sub-key 'k'
        assertEquals(1L, (long) g.V().has("data.k", "fromData").count().next());
        assertEquals(0L, (long) g.V().has("data.k", "fromMeta").count().next());
        assertEquals(1L, (long) g.V().has("meta.k", "fromMeta").count().next());
        // negative
        assertEquals(0L, (long) g.V().has("data.status", "inactive").count().next());
    }
}
