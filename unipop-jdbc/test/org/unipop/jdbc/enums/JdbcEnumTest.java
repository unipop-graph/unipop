package org.unipop.jdbc.enums;

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
 * Exercises a property backed by a PostgreSQL native enum column (write + query + read + update)
 * against embedded PostgreSQL.
 */
public class JdbcEnumTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS people");
            s.execute("DROP TYPE IF EXISTS mood");
            s.execute("CREATE TYPE mood AS ENUM ('active','inactive')");
            s.execute("CREATE TABLE people (id varchar(100) primary key, label varchar(100), name varchar(100), status mood)");
        }
        String dir = new File(JdbcEnumTest.class.getResource("/configuration/enums/people.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "enumtest");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Test
    public void shouldWriteQueryReadAndUpdateEnumColumn() {
        // write
        g.addV("person").property(T.id, "1").property("name", "marko").property("status", "active").next();
        // query + read
        assertEquals(1L, (long) g.V().has("status", "active").count().next());
        assertEquals("active", g.V("1").values("status").next());
        // update
        g.V("1").property("status", "inactive").next();
        assertEquals("inactive", g.V("1").values("status").next());
        assertEquals(0L, (long) g.V().has("status", "active").count().next());
    }
}
