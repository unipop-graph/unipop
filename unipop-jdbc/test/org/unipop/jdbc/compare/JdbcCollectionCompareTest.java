package org.unipop.jdbc.compare;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * A has() value that is a list compares the property to the list itself. jOOQ cannot bind a list as a
 * parameter, so these filters used to fail the query; they must instead match what TinkerGraph matches.
 */
public class JdbcCollectionCompareTest {

    private static final UUID U1 = UUID.fromString("f47ac10b-58cc-4372-a567-0f02b2f3d479");
    private static final List<String> NAMES = Arrays.asList("a", "b");
    private static final List<String> REFS = Arrays.asList(U1.toString(), "x");

    private static GraphTraversalSource g;
    private static GraphTraversalSource tinker;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS collection_compare");
            s.execute("CREATE TABLE collection_compare (id varchar(100) primary key, label varchar(100), name varchar(100), ref uuid)");
            s.execute("INSERT INTO collection_compare VALUES ('1', 'thing', 'a', '" + U1 + "'), ('2', 'thing', 'b', null), ('3', 'thing', null, null)");
        }
        String dir = new File(JdbcCollectionCompareTest.class.getResource("/configuration/collectioncompare/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "collectioncompare");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();

        tinker = TinkerGraph.open().traversal();
        tinker.addV("thing").property(T.id, "1").property("name", "a").property("ref", U1).iterate();
        tinker.addV("thing").property(T.id, "2").property("name", "b").iterate();
        tinker.addV("thing").property(T.id, "3").iterate();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
        if (tinker != null) tinker.close();
    }

    private static void assertMatches(long expected, Function<GraphTraversalSource, Long> query) {
        assertEquals("TinkerGraph", expected, (long) query.apply(tinker));
        assertEquals("unipop", expected, (long) query.apply(g));
    }

    @Test
    public void eqAgainstAListMatchesNothing() {
        assertMatches(0, s -> s.V().has("name", NAMES).count().next());
        assertMatches(0, s -> s.V().has("ref", REFS).count().next());
    }

    @Test
    public void neqAgainstAListMatchesEveryPresentValue() {
        assertMatches(2, s -> s.V().has("name", P.neq(NAMES)).count().next());
        assertMatches(1, s -> s.V().has("ref", P.neq(REFS)).count().next());
    }

    @Test
    public void notEqAgainstAListMatchesEveryPresentValue() {
        assertMatches(2, s -> s.V().has("name", P.not(P.eq(NAMES))).count().next());
        assertMatches(0, s -> s.V().has("name", P.not(P.neq(NAMES))).count().next());
    }

    @Test
    public void orderingAgainstAListMatchesNothingUnlessNegated() {
        assertMatches(0, s -> s.V().has("name", P.gt(NAMES)).count().next());
        assertMatches(0, s -> s.V().has("name", P.lte(NAMES)).count().next());
        assertMatches(2, s -> s.V().has("name", P.not(P.gt(NAMES))).count().next());
    }
}
