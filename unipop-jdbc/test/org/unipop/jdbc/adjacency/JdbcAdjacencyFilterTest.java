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
import java.util.UUID;

import static org.junit.Assert.assertEquals;

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
        assertEquals(1L, (long) g.V("a").out("E").has("org_id", U.toString()).count().next());
        assertEquals(1L, (long) g.V("a").out("E").has("org_id", U).count().next());
        assertEquals(0L, (long) g.V("a").out("E").has("org_id", OTHER.toString()).count().next());
    }

    @Test
    public void partitionStrategyScopesOutHop() {
        GraphTraversalSource in = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(U.toString()).create());
        assertEquals(1L, (long) in.V("a").out("E").count().next());
        GraphTraversalSource out = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(OTHER.toString()).create());
        assertEquals(0L, (long) out.V("a").out("E").count().next());
    }

    @Test
    public void hasAfterOutVFiltersByString() {
        assertEquals(1L, (long) g.E().outV().has("org_id", U.toString()).count().next());
        assertEquals(0L, (long) g.E().outV().has("org_id", OTHER.toString()).count().next());
    }

    @Test
    public void partitionStrategyScopesOutV() {
        GraphTraversalSource in = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(U.toString()).create());
        assertEquals(1L, (long) in.E().outV().count().next());
        GraphTraversalSource out = g.withStrategies(
                PartitionStrategy.build().partitionKey("org_id").readPartitions(OTHER.toString()).create());
        assertEquals(0L, (long) out.E().outV().count().next());
    }
}
