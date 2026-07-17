package org.unipop.jdbc.range;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.process.range.UniGraphRangeStep;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcRangePushdownTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS jr_host");
            s.execute("DROP TABLE IF EXISTS jr_person");
            s.execute("CREATE TABLE jr_host   (id varchar(100) primary key, name varchar(50))");
            s.execute("CREATE TABLE jr_person (id varchar(100) primary key, name varchar(50))");
            // hosts sorted by name: alpha,bravo,charlie,delta,echo  (5 rows)
            s.execute("INSERT INTO jr_host VALUES ('h1','alpha'),('h2','bravo'),('h3','charlie'),('h4','delta'),('h5','echo')");
            s.execute("INSERT INTO jr_person VALUES ('p1','foxtrot'),('p2','golf'),('p3','hotel')");
        }
        String dir = new File(JdbcRangePushdownTest.class.getResource("/configuration/rangepushdown/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "rangepushdown");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception { if (g != null) g.close(); }

    @Before
    public void clearTiming() { TimingExecuterListener.timing.clear(); }

    /** Largest row count any executed query against a table fetched. */
    private static int maxFetch(String table) {
        return TimingExecuterListener.timing.entrySet().stream()
                .filter(e -> e.getKey().toLowerCase().contains(table))
                .mapToInt(e -> e.getValue().getValue1()).max().orElse(-1);
    }

    @Test
    public void strategyReplacesGlobalRangeAfterGraphStep() {
        GraphTraversal<?, ?> t = g.V().hasLabel("host").order().by("name").range(2, 4);
        t.hasNext(); // force strategy application + execution
        assertTrue("expected a UniGraphRangeStep in the compiled traversal",
                TraversalHelper.hasStepOfAssignableClass(UniGraphRangeStep.class, t.asAdmin()));
    }
}
