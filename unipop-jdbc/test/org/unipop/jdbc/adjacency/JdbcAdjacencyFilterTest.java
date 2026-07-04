package org.unipop.jdbc.adjacency;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.unipop.jdbc.utils.TimingExecuterListener;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Vertex-property has() after out()/outV() must filter (push down), enabling PartitionStrategy. */
public class JdbcAdjacencyFilterTest {

    private static GraphTraversalSource g;
    private static final UUID U = UUID.fromString("019ef96c-f001-7c4f-82cc-b2413065f8c1");
    private static final UUID OTHER = UUID.fromString("11111111-1111-4111-8111-111111111111");

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS adjnodes");
            s.execute("DROP TABLE IF EXISTS adjedges");
            s.execute("CREATE TABLE adjnodes (id varchar(100) primary key, label varchar(100), org_id uuid)");
            s.execute("CREATE TABLE adjedges (id varchar(100) primary key, label varchar(100), " +
                    "src_id varchar(100), src_label varchar(100), dst_id varchar(100), dst_label varchar(100), org_id uuid)");
            s.execute("INSERT INTO adjnodes VALUES ('a','n','" + U + "'), ('b','n','" + U + "')");
            s.execute("INSERT INTO adjedges VALUES ('e1','E','a','n','b','n','" + U + "')");
            s.execute("INSERT INTO adjnodes VALUES ('c','n','" + U + "')");
            s.execute("INSERT INTO adjedges VALUES ('e2','E','b','n','c','n','" + U + "')");
            s.execute("INSERT INTO adjedges VALUES ('e3','E','a','n','c','n','" + U + "')");
        }
        String dir = new File(JdbcAdjacencyFilterTest.class.getResource("/configuration/adjfilter/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "adjfilter");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Test
    public void hasAfterOutFiltersByString() {
        assertEquals(2L, (long) g.V("a").out("E").has("org_id", U.toString()).count().next());
        assertEquals(2L, (long) g.V("a").out("E").has("org_id", U).count().next());
        assertEquals(0L, (long) g.V("a").out("E").has("org_id", OTHER.toString()).count().next());
    }

    @Test
    public void partitionStrategyScopesOutHop() {
        GraphTraversalSource in = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(U.toString()).create());
        assertEquals(2L, (long) in.V("a").out("E").count().next());
        GraphTraversalSource out = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(OTHER.toString()).create());
        assertEquals(0L, (long) out.V("a").out("E").count().next());
    }

    @Test
    public void hasAfterOutVFiltersByString() {
        assertEquals(3L, (long) g.E().outV().has("org_id", U.toString()).count().next());
        assertEquals(0L, (long) g.E().outV().has("org_id", OTHER.toString()).count().next());
    }

    @Test
    public void partitionStrategyScopesOutV() {
        GraphTraversalSource in = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(U.toString()).create());
        assertEquals(3L, (long) in.E().outV().count().next());
        GraphTraversalSource out = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(OTHER.toString()).create());
        assertEquals(0L, (long) out.E().outV().count().next());
    }

    @Test
    public void partitionStrategyScopesMultiHop() {
        GraphTraversalSource in = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(U.toString()).create());
        assertEquals(1L, (long) in.V("a").out("E").out("E").count().next());   // a->b->c
        GraphTraversalSource out = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(OTHER.toString()).create());
        assertEquals(0L, (long) out.V("a").out("E").out("E").count().next());
    }

    @Test
    public void hasAfterBothPreservesDuplicateIds() {
        // both() reaches every vertex from multiple incident edges: with e1(a-b),e2(b-c),e3(a-c),
        // g.V().both('E') yields [b,c, a,c, b,a] = 6 traversers (each id appears twice). The filtered
        // fetch dedups by id and un-defers one instance per id; the drop must keep BOTH instances of
        // each matching id, not collapse to the distinct-id count (3).
        assertEquals(6L, (long) g.V().both("E").has("org_id", U.toString()).count().next());
        assertEquals(0L, (long) g.V().both("E").has("org_id", OTHER.toString()).count().next());
    }

    @Test
    public void hasAfterOtherVPreservesDuplicateIds() {
        // g.V().bothE('E').otherV() also yields 6 traversers with each id twice (EdgeOtherVertexStep).
        assertEquals(6L, (long) g.V().bothE("E").otherV().has("org_id", U.toString()).count().next());
        assertEquals(0L, (long) g.V().bothE("E").otherV().has("org_id", OTHER.toString()).count().next());
    }

    @Test
    public void coalesceWorksAsByModulator() {
        // 1. project().by(coalesce(...)) — was ClassCastException; now first-branch-wins => 1
        List<Map<String, Object>> proj = g.V().project("w")
                .by(coalesce(constant(1), constant(2))).toList();
        assertEquals(3, proj.size());
        proj.forEach(m -> assertEquals(1, m.get("w")));

        // 2. order().by(coalesce(...)) — was CCE; now no throw, all 3 vertices returned
        assertEquals(3L, (long) g.V().order().by(coalesce(constant(1), constant(2))).count().next());

        // 3+4. local()/union() KEEP ALL branch outputs — proves correct first-branch-wins
        //      (the rejected interface-swap returned [1,2,1,2,1,2] here).
        assertEquals(List.of(1, 1, 1), g.V().local(coalesce(constant(1), constant(2))).toList());
        assertEquals(List.of(1, 1, 1), g.V().union(coalesce(constant(1), constant(2))).toList());

        // 5. main-step coalesce (root) — unchanged, still handled by UniGraphCoalesceStep
        assertEquals(List.of(1, 1, 1), g.V().coalesce(constant(1), constant(2)).toList());

        // 6. child-branch push-down + fallback preserved through native coalesce:
        //    b has an out edge (e2 b->c) -> out('E').id() wins => "c";
        //    c has no out edge -> coalesce falls through to the constant => "noedge".
        assertEquals("c", g.V("b").project("n")
                .by(coalesce(out("E").id(), constant("noedge"))).next().get("n"));
        assertEquals("noedge", g.V("c").project("n")
                .by(coalesce(out("E").id(), constant("noedge"))).next().get("n"));

        // 7. values()+constant modulator (the reported failing shape) — org_id present => value wins
        assertEquals(U, g.V("a").project("w")
                .by(coalesce(values("org_id"), constant(false))).next().get("w"));
    }

    /** Largest number of rows any executed query against the adjnodes table fetched. */
    private static int maxAdjnodesFetch() {
        return TimingExecuterListener.timing.entrySet().stream()
                .filter(e -> e.getKey().toLowerCase().contains("adjnodes"))
                .mapToInt(e -> e.getValue().getValue1())
                .max().orElse(-1);
    }

    /**
     * The deferred vertex fetch must be bounded to the produced neighbour ids, not scan the whole
     * table (hydrate-only path) or the whole partition (pushed-predicate path). Proven by inserting
     * same-partition non-neighbour "poison" rows and asserting the fetch reads only the neighbour.
     * g.E("e1").inV() isolates adjnodes to a single query (the deferred fetch of b).
     */
    @Test
    public void deferredFetchIsBoundedToProducedIds() throws Exception {
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            for (int i = 0; i < 30; i++) {
                s.execute("INSERT INTO adjnodes VALUES ('p" + i + "','n','" + U + "')");
            }
        }
        try {
            // Hydrate-only path (no pushed predicate): before the fix this was WHERE true (full scan).
            TimingExecuterListener.timing.clear();
            List<Object> vals = g.E("e1").inV().values("org_id").toList();
            assertEquals(1, vals.size());
            assertEquals(U, vals.get(0));
            assertTrue("hydrate-only deferred fetch scanned " + maxAdjnodesFetch()
                    + " adjnodes rows; must be bounded to the produced id", maxAdjnodesFetch() <= 2);

            // Pushed-predicate path: before the fix this was WHERE org_id = ? (partition scan).
            TimingExecuterListener.timing.clear();
            assertEquals(1L, (long) g.E("e1").inV().has("org_id", U.toString()).count().next());
            assertTrue("pushed-predicate deferred fetch scanned " + maxAdjnodesFetch()
                    + " adjnodes rows; must be bounded to the produced id", maxAdjnodesFetch() <= 2);
        } finally {
            try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
                 Statement s = c.createStatement()) {
                s.execute("DELETE FROM adjnodes WHERE id LIKE 'p%'");
            }
        }
    }
}
