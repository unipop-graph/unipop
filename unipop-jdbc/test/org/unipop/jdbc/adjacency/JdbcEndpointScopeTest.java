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
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Two tenants hold vertices with the same ids. With "endpointScope": ["tenant"] on the edge schema,
 * a tenant-scoped adjacency hop must follow only that tenant's edges, on the join push-down and on
 * the deferred path alike.
 */
public class JdbcEndpointScopeTest {

    private static final UUID T1 = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID T2 = UUID.fromString("22222222-2222-4222-8222-222222222222");
    private static Graph graph;
    private static Graph graphNoJoin;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS es_nodes");
            s.execute("DROP TABLE IF EXISTS es_edges");
            s.execute("CREATE TABLE es_nodes (tenant_id uuid, id varchar(100), label varchar(100), name varchar(100), primary key (tenant_id, id))");
            s.execute("CREATE TABLE es_edges (id varchar(100) primary key, label varchar(100), tenant_id uuid, " +
                    "src_id varchar(100), src_label varchar(100), dst_id varchar(100), dst_label varchar(100))");
            for (UUID t : Arrays.asList(T1, T2)) {
                String n = t.equals(T1) ? "1" : "2";
                s.execute("INSERT INTO es_nodes VALUES ('" + t + "','a','n','a" + n + "'),('" + t + "','b','n','b" + n + "'),('" + t + "','c','n','c" + n + "')");
            }
            // T1: a->b. T2: a->c, b->c. Every T2 edge joins ids that T1 also holds.
            s.execute("INSERT INTO es_edges VALUES ('e1','E','" + T1 + "','a','n','b','n')");
            s.execute("INSERT INTO es_edges VALUES ('e2','E','" + T2 + "','a','n','c','n')");
            s.execute("INSERT INTO es_edges VALUES ('e3','E','" + T2 + "','b','n','c','n')");
        }
        graph = open(true);
        graphNoJoin = open(false);
    }

    private static Graph open(boolean joinPushdown) throws Exception {
        String dir = new File(JdbcEndpointScopeTest.class.getResource("/configuration/endpointscope/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "endpointscope");
        conf.setProperty("providers", dir);
        conf.setProperty("jdbc.joinPushdown", joinPushdown);
        return UniGraph.open(conf);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (graph != null) graph.close();
        if (graphNoJoin != null) graphNoJoin.close();
    }

    private static GraphTraversalSource scoped(Graph g, UUID tenant) {
        return g.traversal().withStrategies(
                PartitionStrategy.build().partitionKey("tenant").readPartitions(tenant.toString()).create());
    }

    private static List<Object> sorted(List<Object> values) {
        return values.stream().map(Object::toString).sorted().collect(Collectors.toList());
    }

    private void assertHopsScoped(Graph g) {
        GraphTraversalSource g1 = scoped(g, T1);
        assertEquals(Collections.singletonList("b1"), g1.V("a").out("E").values("name").toList());
        assertEquals(Collections.emptyList(), g1.V("c").in("E").values("name").toList());
        assertEquals(Collections.singletonList("a1"), g1.V("b").both("E").values("name").toList());
        assertEquals(Collections.emptyList(), g1.V("a").out("E").out("E").values("name").toList());
        assertEquals(1L, (long) g1.V("a").repeat(out("E")).emit().path().count().next());

        GraphTraversalSource g2 = scoped(g, T2);
        assertEquals(Collections.singletonList("c2"), g2.V("a").out("E").values("name").toList());
        assertEquals(Arrays.asList("a2", "b2"), sorted(g2.V("c").in("E").values("name").toList()));
    }

    @Test
    public void joinPushdownFollowsOnlyTheTenantsEdges() {
        TimingExecuterListener.timing.clear();
        assertHopsScoped(graph);
        assertTrue("the join path must have run with the scope condition",
                TimingExecuterListener.timing.keySet().stream().map(String::toLowerCase)
                        .anyMatch(sql -> sql.contains("join") && sql.contains("e.tenant_id = v.tenant_id")));
    }

    @Test
    public void deferredPathFollowsOnlyTheTenantsEdges() {
        TimingExecuterListener.timing.clear();
        assertHopsScoped(graphNoJoin);
        assertTrue("join push-down is off, so no query may join",
                TimingExecuterListener.timing.keySet().stream().noneMatch(sql -> sql.toLowerCase().contains("join")));
    }

    @Test
    public void edgeStepsFollowOnlyTheTenantsEdges() {
        assertEquals(Collections.singletonList("e1"), scoped(graph, T1).V("a").outE("E").id().toList());
        assertEquals(Arrays.asList("e2", "e3"), sorted(scoped(graph, T2).V("c").inE("E").id().toList()));
    }

    @Test
    public void edgeEndpointLoadsTheEdgesTenantRow() {
        // Unscoped, so only the endpoint fetch's scope keeps T1's same-id vertex out.
        assertEquals(Collections.singletonList("c2"), graph.traversal().E("e2").inV().values("name").toList());
        assertEquals(Collections.singletonList("a1"), graph.traversal().E("e1").outV().values("name").toList());
        assertEquals(Arrays.asList("a2", "b2"), sorted(scoped(graph, T2).V("c").inE("E").outV().values("name").toList()));
    }

    @Test
    public void edgeHopPathsFetchOnlyTheTenantsVertices() {
        TimingExecuterListener.timing.clear();
        GraphTraversalSource g1 = scoped(graph, T1);
        assertEquals(Collections.singletonList("b1"), g1.V("a").bothE("E").otherV().values("name").toList());
        assertEquals(1L, (long) g1.V("a").repeat(bothE("E").otherV().simplePath()).emit().path().count().next());
        GraphTraversalSource g2 = scoped(graph, T2);
        assertEquals(Arrays.asList("b2", "c2"),
                sorted(g2.V("a").repeat(bothE("E").otherV().simplePath()).emit().values("name").toList()));
        List<String> unscopedNodeReads = TimingExecuterListener.timing.keySet().stream().map(String::toLowerCase)
                .filter(sql -> sql.contains("from es_nodes") && !sql.contains("tenant_id"))
                .collect(Collectors.toList());
        assertEquals(Collections.emptyList(), unscopedNodeReads);
    }
}
