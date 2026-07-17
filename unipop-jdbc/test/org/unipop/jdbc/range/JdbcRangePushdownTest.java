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

    @Test
    public void singleSchemaPushesOffset() {
        // hosts by name: alpha,bravo,charlie,delta,echo ; range(2,4) -> charlie,delta
        List<Object> names = g.V().hasLabel("host").order().by("name").range(2, 4).values("name").toList();
        assertEquals(java.util.Arrays.asList("charlie", "delta"), names);
        // OFFSET pushed -> only 2 rows fetched from jr_host (not 4)
        assertEquals("expected OFFSET pushed: 2 rows fetched, not 4", 2, maxFetch("jr_host"));
    }

    @Test
    public void skipPushesOffsetUnbounded() {
        // skip(3) -> delta,echo ; OFFSET 3 no limit -> 2 rows fetched
        List<Object> names = g.V().hasLabel("host").order().by("name").skip(3).values("name").toList();
        assertEquals(java.util.Arrays.asList("delta", "echo"), names);
        assertEquals(2, maxFetch("jr_host"));
    }

    @Test
    public void pureLimitUnchanged() {
        // limit(2) -> alpha,bravo ; offset=0 -> today's fetch-reduction (LIMIT 2), 2 rows
        List<Object> names = g.V().hasLabel("host").order().by("name").limit(2).values("name").toList();
        assertEquals(java.util.Arrays.asList("alpha", "bravo"), names);
        assertEquals(2, maxFetch("jr_host"));
    }

    @Test
    public void dedupBetweenSourceAndRangeDoesNotPushOffset() {
        // dedup() between g.V() and range() must NOT push OFFSET (dedup isn't pushed to SQL, so
        // OFFSET over raw rows would skip the wrong rows). Correct window size, and OFFSET not pushed.
        long n = g.V().hasLabel("host").dedup().range(1, 3).count().next();
        assertEquals(2L, n);                       // window size (5 distinct hosts -> range(1,3) = 2)
        assertTrue("dedup-intervening must not push OFFSET (fetches all, not 2)", maxFetch("jr_host") > 2);
    }

    @Test
    public void multiSchemaFanOutDoesNotPushOffset() {
        // No hasLabel -> host(5)+person(3)=8 by name: alpha,bravo,charlie,delta,echo,foxtrot,golf,hotel
        // range(2,4) -> charlie,delta
        List<Object> names = g.V().order().by("name").range(2, 4).values("name").toList();
        assertEquals(java.util.Arrays.asList("charlie", "delta"), names);
        // offset NOT pushed: each schema fetch-reduces with LIMIT 4 (fetches up to 4, not 2)
        assertTrue("multi-schema must not push OFFSET (in-memory residual)", maxFetch("jr_host") > 2);
    }

    @Test
    public void orderlessSingleSchemaRangeReturnsWindowSize() {
        // range(1,4) with no order() -> 3 elements (spec-correct arbitrary window)
        long n = g.V().hasLabel("host").range(1, 4).count().next();
        assertEquals(3L, n);
    }

    @Test
    public void labelOnRangeStepIsPreserved() {
        // If replaceStep dropped the 'a' label, select('a') would fail/empty.
        List<Object> names = g.V().hasLabel("host").order().by("name").limit(2).as("a")
                .select("a").values("name").toList();
        assertEquals(java.util.Arrays.asList("alpha", "bravo"), names);
    }

    @Test
    public void localRangeStaysNative() {
        // range(local,...) must NOT be replaced -> no UniGraphRangeStep for the local range
        GraphTraversal<?, ?> t = g.V().hasLabel("host").fold()
                .range(org.apache.tinkerpop.gremlin.process.traversal.Scope.local, 0, 2);
        t.hasNext();
        assertTrue("local range must remain native",
                !TraversalHelper.hasStepOfAssignableClass(UniGraphRangeStep.class, t.asAdmin()));
    }
}
