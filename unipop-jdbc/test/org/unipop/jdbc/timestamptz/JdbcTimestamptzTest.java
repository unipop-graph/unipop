package org.unipop.jdbc.timestamptz;

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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises a property backed by a PostgreSQL native timestamptz column (write + read + range
 * query + update), with OffsetDateTime and ISO-string values, against embedded PostgreSQL.
 */
public class JdbcTimestamptzTest {

    private static GraphTraversalSource g;
    private static final OffsetDateTime T1 = OffsetDateTime.parse("2020-01-01T00:00:00Z");
    private static final OffsetDateTime T2 = OffsetDateTime.parse("2021-06-15T12:00:00Z");
    private static final OffsetDateTime T3 = OffsetDateTime.parse("2022-12-31T23:59:59Z");

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS tstz");
            s.execute("CREATE TABLE tstz (id varchar(100) primary key, label varchar(100), name varchar(100), at timestamptz)");
        }
        String dir = new File(JdbcTimestamptzTest.class.getResource("/configuration/timestamptz/tstz.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "tstztest");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @Before
    public void clean() throws Exception {
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("TRUNCATE TABLE tstz");
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Test
    public void writeOffsetDateTimeReadBackAndRangeQuery() {
        g.addV("thing").property(T.id, "1").property("name", "a").property("at", T1).next();
        g.addV("thing").property(T.id, "2").property("name", "b").property("at", T2).next();
        g.addV("thing").property(T.id, "3").property("name", "c").property("at", T3).next();

        Object read = g.V("2").values("at").next();
        assertTrue("values(at) must be a java.time.OffsetDateTime", read instanceof OffsetDateTime);
        // Normalize both to UTC for comparison (JDBC driver may return with session timezone)
        OffsetDateTime readUTC = ((OffsetDateTime) read).withOffsetSameInstant(ZoneOffset.UTC);
        assertEquals(T2, readUTC);

        assertEquals(1L, (long) g.V().has("at", T2).count().next());          // eq
        assertEquals(1L, (long) g.V().has("at", P.gt(T2)).count().next());     // T3
        assertEquals(1L, (long) g.V().has("at", P.lt(T2)).count().next());     // T1
        assertEquals(2L, (long) g.V().has("at", P.between(T1, T3)).count().next()); // T1, T2 (half-open)
    }

    @Test
    public void writeIsoStringReadBackAsOffsetDateTimeAndQueryByString() {
        g.addV("thing").property(T.id, "4").property("name", "d").property("at", "2019-03-03T08:30:00Z").next();
        Object read = g.V("4").values("at").next();
        assertTrue(read instanceof OffsetDateTime);
        // Normalize to UTC for comparison (JDBC driver may return with session timezone)
        OffsetDateTime readUTC = ((OffsetDateTime) read).withOffsetSameInstant(ZoneOffset.UTC);
        assertEquals(OffsetDateTime.parse("2019-03-03T08:30:00Z"), readUTC);
        assertEquals(1L, (long) g.V().has("at", "2019-03-03T08:30:00Z").count().next()); // string predicate coerced
    }

    @Test
    public void updateTimestamptz() {
        g.addV("thing").property(T.id, "5").property("name", "e").property("at", T1).next();
        g.V("5").property("at", T3).next();
        Object read = g.V("5").values("at").next();
        assertTrue(read instanceof OffsetDateTime);
        // Normalize to UTC for comparison (JDBC driver may return with session timezone)
        OffsetDateTime readUTC = ((OffsetDateTime) read).withOffsetSameInstant(ZoneOffset.UTC);
        assertEquals(T3, readUTC);
        assertEquals(1L, (long) g.V().has("at", P.gt(T2)).count().next()); // now T3
        assertEquals(0L, (long) g.V().has("at", P.lt(T2)).count().next());
    }
}
