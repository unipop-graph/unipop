package org.unipop.jdbc.union;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
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
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcUnionTest {

    private static GraphTraversalSource g;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS u_node");
            s.execute("DROP TABLE IF EXISTS u_link");
            s.execute("CREATE TABLE u_node (id varchar(100) primary key, name varchar(50))");
            s.execute("CREATE TABLE u_link (id varchar(100) primary key, src_id varchar(100), dst_id varchar(100))");
            s.execute("INSERT INTO u_node VALUES ('n1','a'),('n2','b'),('n3','c'),('n4','d')");
            s.execute("INSERT INTO u_link VALUES ('e1','n1','n2'),('e2','n1','n3'),('e3','n2','n4')");
        }
        String dir = new File(JdbcUnionTest.class.getResource("/configuration/union/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "union");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception { if (g != null) g.close(); }

    @Before
    public void clearTiming() { TimingExecuterListener.timing.clear(); }

    /** Distinct executed SELECT queries touching u_link (batching issues one IN(...) query per hop,
     *  not one query per input traverser). */
    private static long linkQueries() {
        return TimingExecuterListener.timing.keySet().stream()
                .filter(sql -> sql.toLowerCase().contains("u_link")).count();
    }

    @Test
    public void unionBatchesBranchQueryPerHop() {
        // The whole point of the batched step: feeding all inputs to a branch lets its UniGraphVertexStep
        // issue ONE IN(...) query for the batch, not one round-trip per input traverser.
        List<Object> ends = g.V().union(__.out("link")).values("name").order().toList();
        assertEquals(java.util.Arrays.asList("b", "c", "d"), ends); // n1->n2,n3 ; n2->n4
        assertEquals("expected one batched u_link query, not one per input", 1L, linkQueries());
        assertEquals(1L, TimingExecuterListener.timing.keySet().stream()
                .filter(sql -> sql.toLowerCase().contains("u_link") && sql.toLowerCase().contains(" in (")).count());
    }

    @Test
    public void unionPreservesPathAndSideEffects() {
        // path() through a union must keep per-traverser path (the old collapse-by-element broke this).
        // n1 -> out(link) = n2,n3 ; out().out() from n1 = n4. Assert the end-vertex names.
        List<Object> ends = g.V("n1").union(__.out("link"), __.out("link").out("link"))
                .values("name").order().toList();
        assertEquals(java.util.Arrays.asList("b", "c", "d"), ends); // n2=b, n3=c, n4=d
    }

    @Test
    public void unionSelectSideEffectSurvives() {
        // union(select(m)...) shape analogous to the mergeE errors: the selected value must survive.
        List<Object> names = g.V("n1").as("m")
                .union(__.select("m").values("name"), __.out("link").values("name"))
                .order().toList();
        // select("m") -> n1(a) ; out(link) -> n2(b),n3(c)
        assertEquals(java.util.Arrays.asList("a", "b", "c"), names);
    }

    @Test
    public void rootUnionSelfSeedsFromGraphStep() {
        // Root g.union(...) is a start step: native UnionStep(isStart=true) self-seeds one starter.
        // Without that seeding the batched step yields nothing (UniBulkStep needs an incoming start).
        assertEquals(Long.valueOf(4), g.union(__.V()).count().next());
        assertEquals(java.util.Arrays.asList("a", "b", "c", "d"),
                g.union(__.V().values("name")).order().toList());
    }

    @Test
    public void rootUnionInjectDropsStarter() {
        // inject() is a start step even mid-traversal; the starter marker passes through it and MUST be
        // filtered from the output (else result would contain the UNION_STARTER object).
        assertEquals(java.util.Arrays.asList(1, 2),
                g.union(__.inject(1), __.inject(2)).order().toList());
    }

    @Test
    public void rootUnionPathHasNoStarter() {
        // The starter's path is dropped so the branch path starts at the branch's own first element.
        // by("name") would throw "can only be applied to an Element/Map" if the starter leaked in.
        List<String> paths = g.union(__.V("n1").out("link").out("link"), __.V("n4"))
                .path().by("name").toList().stream()
                .map(Object::toString).sorted().collect(java.util.stream.Collectors.toList());
        assertEquals(java.util.Arrays.asList("path[a, b, d]", "path[d]"), paths);
    }

    @Test
    public void unionPreservesBulkMultiplicity() {
        // g.V().out().in() yields n1 twice (via n2, via n3); an upstream barrier merges them into a
        // bulked traverser. Draining the branch via next() would bulk-expand and double-count; draining
        // the branch end step preserves bulk. count must equal the same shape without union.
        long plain = g.V().out("link").in("link").out("link").count().next();
        long union = g.V().out("link").in("link").union(__.out("link")).count().next();
        assertEquals(plain, union);
        assertEquals(5L, union);
    }

    @Test
    public void unionGroupCountMultiplicity() {
        // groupCount over a union: sum of counts must equal the number of union outputs (no inflation).
        // out(link)=3 (n2,n3,n4) + out(link).in(link)=3 (n1,n1,n2) = 6.
        long sum = g.V().union(__.out("link"), __.out("link").in("link"))
                .<Object>groupCount().select(Column.values).unfold().sum().next().longValue();
        assertEquals(6L, sum);
    }

    @Test
    public void unionNestedInChooseDoesNotSelfSeed() {
        // A union that starts a child option traversal (here under choose) still receives real parent
        // input and must NOT self-seed a starter (isStart is restricted to top-level root unions).
        List<String> r = g.V()
                .<String>choose(__.has("name", "a"), __.union(__.out("link").values("name")), __.constant("z"))
                .order().toList();
        // n1(a) -> union(out.name) = b,c ; n2/n3/n4 -> constant z
        assertEquals(java.util.Arrays.asList("b", "c", "z", "z", "z"), r);
    }
}
