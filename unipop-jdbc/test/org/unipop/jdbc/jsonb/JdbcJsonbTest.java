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
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Exercises a schemaless JSONB catch-all column on embedded PostgreSQL: store scalar + nested
 * properties, query top-level and nested keys, and read values back.
 */
public class JdbcJsonbTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS docs");
            s.execute("CREATE TABLE docs (id varchar(100) primary key, label varchar(100), name varchar(100), data jsonb)");
        }
        String dir = new File(JdbcJsonbTest.class.getResource("/configuration/jsonb/docs.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "jsonbtest");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    /**
     * A jsonb sub-key whose value is an empty array [] must not throw from a single-value accessor
     * (project(...).by(key) / value(key)); it should behave like no-value, matching values(). A
     * non-empty array still yields its first element. Regression for the unchecked get(0)
     * (IndexOutOfBoundsException) on empty-array sub-keys.
     */
    @Test
    public void emptyJsonbArraySubKeyIsNoValueNotIndexError() throws Exception {
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("INSERT INTO docs (id, label, name, data) VALUES " +
                    "('empty', 'doc', 'e', '{\"tags\": []}'::jsonb), " +
                    "('full',  'doc', 'f', '{\"tags\": [\"a\", \"b\"]}'::jsonb)");
        }

        // empty array -> no value -> project omits the key (no exception)
        Map<String, Object> emptyRow = g.V("empty").project("x").by("data.tags").next();
        assertFalse("empty jsonb array sub-key must map to no entry", emptyRow.containsKey("x"));
        // and values() likewise emits nothing
        assertFalse(g.V("empty").values("data.tags").hasNext());

        // non-empty array still returns element 0 (no regression)
        Map<String, Object> fullRow = g.V("full").project("x").by("data.tags").next();
        assertEquals("a", fullRow.get("x"));
    }

    @Test
    public void shouldStoreQueryAndReadSchemalessJsonb() {
        Map<String, Object> address = Collections.singletonMap("city", "NYC");
        g.addV("doc").property(T.id, "1").property("name", "a")
                .property("data.status", "active").property("data.address", address).next();

        // top-level key query
        assertEquals(1L, (long) g.V().has("data.status", "active").count().next());
        // nested-path key query
        assertEquals(1L, (long) g.V().has("data.address.city", "NYC").count().next());
        // read-back
        assertEquals("active", g.V("1").values("data.status").next());
        // whole JSONB object under bare column key
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) g.V("1").values("data").next();
        assertEquals("active", data.get("status"));
        assertEquals("NYC", ((Map<?, ?>) data.get("address")).get("city"));
        // negative
        assertEquals(0L, (long) g.V().has("data.status", "inactive").count().next());
    }
}
