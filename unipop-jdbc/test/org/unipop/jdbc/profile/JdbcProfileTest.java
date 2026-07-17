package org.unipop.jdbc.profile;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Issue #108: under profile(), each schema's child metric must report ITS OWN query's count/duration.
 * Bug: RowController.fillChildren assigned children by positional index into the static, never-cleared,
 * unordered TimingExecuterListener.timing.values() (contaminated by every prior query) instead of
 * keying by each schema's SQL — so child counts/durations were arbitrary/stale and didn't add up to
 * the parent.
 */
public class JdbcProfileTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS pf_host");
            s.execute("DROP TABLE IF EXISTS pf_person");
            s.execute("CREATE TABLE pf_host   (id varchar(100) primary key, name varchar(50))");
            s.execute("CREATE TABLE pf_person (id varchar(100) primary key, name varchar(50))");
            s.execute("INSERT INTO pf_host VALUES ('h1','a'),('h2','b'),('h3','c'),('h4','d'),('h5','e')"); // 5 hosts
            s.execute("INSERT INTO pf_person VALUES ('p1','f'),('p2','g'),('p3','h')");                       // 3 persons
        }
        String dir = new File(JdbcProfileTest.class.getResource("/configuration/profile/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "profile");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception { if (g != null) g.close(); }

    @Before
    public void seedStaleTimings() {
        // Real usage never clears the static global timing map, so it accumulates entries from every
        // prior query. Simulate that contamination so the child<->schema association is exercised
        // against a map that is NOT just this query's two entries.
        TimingExecuterListener.timing.clear();
        for (int i = 0; i < 16; i++) TimingExecuterListener.timing.put("POISON_" + i, new Pair<>(111L, 9999));
    }

    /** All leaf metrics (no further nesting) — the per-schema children. */
    private static List<Metrics> leaves(TraversalMetrics tm) {
        List<Metrics> out = new ArrayList<>();
        for (Metrics m : tm.getMetrics()) collectLeaves(m, out);
        return out;
    }

    private static void collectLeaves(Metrics m, List<Metrics> out) {
        if (m.getNested().isEmpty()) out.add(m);
        else for (Metrics n : m.getNested()) collectLeaves(n, out);
    }

    private static Metrics leafForTable(TraversalMetrics tm, String table) {
        return leaves(tm).stream().filter(m -> m.getName().contains("table='" + table + "'")).findFirst().orElse(null);
    }

    private static Metrics controllerMetric(TraversalMetrics tm) {
        for (Metrics step : tm.getMetrics())
            for (Metrics nested : step.getNested())
                if (nested.getName().contains("RowController")) return nested;
        return null;
    }

    @Test
    public void schemaChildMetricsReportTheirOwnSchemaCount() {
        TraversalMetrics tm = g.V().profile().next();

        Metrics host = leafForTable(tm, "pf_host");
        Metrics person = leafForTable(tm, "pf_person");
        assertNotNull("host schema child metric missing", host);
        assertNotNull("person schema child metric missing", person);

        assertEquals("host schema child must report its own row count (5), not a stale/other query's",
                5L, host.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
        assertEquals("person schema child must report its own row count (3), not a stale/other query's",
                3L, person.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());

        // The controller (parent of the schema children) counts the sum of its children — which must
        // therefore add up to the real total, not the poison total.
        Metrics controller = controllerMetric(tm);
        assertNotNull("controller metric missing", controller);
        assertEquals("controller count must equal the sum of its schema children (5+3), not poison",
                8L, controller.getCount(TraversalMetrics.ELEMENT_COUNT_ID).longValue());
    }
}
