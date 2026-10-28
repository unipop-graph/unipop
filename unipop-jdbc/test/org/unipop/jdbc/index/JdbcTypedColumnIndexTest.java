package org.unipop.jdbc.index;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * eq/within on uuid and enum columns must leave the column uncast so a btree index on it applies
 * ({@code col::text = ?} cannot use one), while matching exactly the rows the text comparison did.
 */
public class JdbcTypedColumnIndexTest {

    private static final int ROWS = 20000;
    private static GraphTraversalSource g;
    private static UUID ref7;
    private static UUID ref8;

    @BeforeClass
    public static void setUp() throws Exception {
        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ti_things");
            s.execute("DROP TYPE IF EXISTS ti_phase");
            s.execute("DROP TYPE IF EXISTS ti_mood");
            s.execute("CREATE TYPE ti_phase AS ENUM ('common', 'rare', 'unused')");
            s.execute("CREATE TYPE ti_mood AS ENUM ('calm', 'angry')");
            s.execute("CREATE TABLE ti_things (id varchar(100) primary key, label varchar(100), ref uuid, phase ti_phase, mood ti_mood)");
            s.execute("INSERT INTO ti_things SELECT i::text, 'thing', md5(i::text)::uuid,"
                    + " CASE WHEN i % 1000 = 0 THEN 'rare' ELSE 'common' END::ti_phase,"
                    + " CASE WHEN i % 2 = 0 THEN 'calm' ELSE 'angry' END::ti_mood"
                    + " FROM generate_series(1, " + ROWS + ") i");
            s.execute("INSERT INTO ti_things VALUES ('null-row', 'thing', NULL, NULL, NULL)");
            s.execute("CREATE INDEX ti_things_ref ON ti_things (ref)");
            s.execute("CREATE INDEX ti_things_phase ON ti_things (phase)");
            s.execute("ANALYZE ti_things");
            ref7 = refOf(s, "7");
            ref8 = refOf(s, "8");
        }
        String dir = new File(JdbcTypedColumnIndexTest.class.getResource("/configuration/typedindex/graph.json").toURI()).getParent();
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty("graphName", "typedindex");
        conf.setProperty("providers", dir);
        g = UniGraph.open(conf).traversal();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (g != null) g.close();
    }

    @Before
    public void reset() {
        // a failed statement leaves the graph's per-thread transaction aborted for the next test
        if (g.getGraph().tx().isOpen()) g.getGraph().tx().rollback();
        TimingExecuterListener.timing.clear();
    }

    private static Connection connect() throws Exception {
        return DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
    }

    private static UUID refOf(Statement s, String id) throws Exception {
        try (ResultSet rs = s.executeQuery("SELECT ref FROM ti_things WHERE id = '" + id + "'")) {
            rs.next();
            return (UUID) rs.getObject(1);
        }
    }

    /** Ids the old text comparison matched, e.g. {@code ref::text = '...'}. */
    private static List<String> idsWhere(String textWhere) throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM ti_things WHERE " + textWhere + " ORDER BY id")) {
            List<String> ids = new java.util.ArrayList<>();
            while (rs.next()) ids.add(rs.getString(1));
            return ids;
        }
    }

    private static List<String> graphIds(Supplier<List<Object>> query) {
        return query.get().stream().map(Object::toString).sorted().collect(Collectors.toList());
    }

    /** The one statement the graph ran against ti_things, with its values inlined. */
    private static String executedSql() {
        List<String> sqls = TimingExecuterListener.timing.keySet().stream()
                .filter(k -> k.contains("ti_things")).collect(Collectors.toList());
        assertEquals("expected one statement against ti_things, got " + sqls, 1, sqls.size());
        return sqls.get(0);
    }

    /** The plan with sequential scans priced out, so only an applicable index can avoid one. */
    private static String explain(String sql) throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("SET enable_seqscan = off");
            StringBuilder plan = new StringBuilder();
            try (ResultSet rs = s.executeQuery("EXPLAIN " + sql)) {
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
            }
            return plan.toString();
        }
    }

    private static void assertIndexed(String sql) throws Exception {
        String plan = explain(sql);
        assertFalse("expected an index scan for " + sql + "\n" + plan, plan.contains("Seq Scan"));
        assertTrue("expected an index scan for " + sql + "\n" + plan, plan.contains("Index Scan") || plan.contains("Index Only Scan"));
    }

    private static String render(String key, P<?> predicate) {
        JdbcPredicatesTranslator t = new JdbcPredicatesTranslator(Collections.emptySet(),
                new java.util.HashSet<>(Arrays.asList("phase", "mood")), Collections.singletonMap("phase", "ti_phase"),
                Collections.emptySet(), Collections.singleton("ref"), Collections.emptySet(), null);
        Condition c = t.translate(PredicatesHolderFactory.predicate(new HasContainer(key, predicate)));
        return DSL.using(SQLDialect.POSTGRES).renderInlined(c);
    }

    @Test
    public void uuidEqualityCastsTheParameterNotTheColumn() {
        String u = ref7.toString();
        assertEquals("ref = '" + u + "'::uuid", render("ref", P.eq(ref7)));
        assertEquals("ref in ('" + u + "'::uuid, '" + ref8 + "'::uuid)", render("ref", P.within(ref7, ref8)));
        assertEquals("(ref = '" + u + "'::uuid or ref = '" + ref8 + "'::uuid)", render("ref", P.eq(ref7).or(P.eq(ref8))));
    }

    @Test
    public void enumEqualityResolvesTheLabelThroughTheEnumType() {
        String rare = "(enum_range(null::ti_phase))[array_position(enum_range(null::ti_phase)::text[], 'rare'::text)]";
        String common = "(enum_range(null::ti_phase))[array_position(enum_range(null::ti_phase)::text[], 'common'::text)]";
        assertEquals("phase = " + rare, render("phase", P.eq("rare")));
        assertEquals("phase in (" + rare + ", " + common + ")", render("phase", P.within("rare", "common")));
    }

    @Test
    public void otherShapesKeepTheTextComparison() {
        // <>, NOT IN and text matching gain nothing from a btree; an enum without "enumType" has no
        // type to resolve labels through; a non-canonical uuid spelling never equalled ref::text.
        assertEquals("\"ref\"::text <> '" + ref7 + "'", render("ref", P.neq(ref7.toString())));
        assertEquals("\"ref\"::text not in ('" + ref7 + "')", render("ref", P.without(ref7.toString())));
        assertEquals("\"phase\"::text <> 'rare'", render("phase", P.neq("rare")));
        assertEquals("\"mood\"::text = 'calm'", render("mood", P.eq("calm")));
        assertEquals("\"ref\"::text = 'NOT-A-UUID'", render("ref", P.eq("NOT-A-UUID")));
        assertEquals("\"ref\"::text = '" + ref7.toString().toUpperCase() + "'", render("ref", P.eq(ref7.toString().toUpperCase())));
    }

    @Test
    public void uuidEqUsesTheIndexAndMatchesTheTextComparison() throws Exception {
        assertEquals(idsWhere("ref::text = '" + ref7 + "'"), graphIds(() -> g.V().has("ref", ref7).id().toList()));
        String sql = executedSql();
        assertFalse(sql, sql.contains("::text"));
        assertIndexed(sql);
        assertEquals(Collections.singletonList("7"), graphIds(() -> g.V().has("ref", ref7.toString()).id().toList()));
    }

    @Test
    public void uuidWithinUsesTheIndexAndMatchesTheTextComparison() throws Exception {
        assertEquals(idsWhere("ref::text in ('" + ref7 + "', '" + ref8 + "')"),
                graphIds(() -> g.V().has("ref", P.within(ref7, ref8)).id().toList()));
        assertIndexed(executedSql());
    }

    @Test
    public void uuidOrUsesTheIndexAndMatchesTheTextComparison() throws Exception {
        assertEquals(Arrays.asList("7", "8"), graphIds(() -> g.V().has("ref", P.eq(ref7).or(P.eq(ref8))).id().toList()));
        assertIndexed(executedSql());
    }

    @Test
    public void enumEqAndWithinUseTheIndexAndMatchTheTextComparison() throws Exception {
        List<String> rare = idsWhere("phase::text = 'rare'");
        assertEquals(ROWS / 1000, rare.size());
        assertEquals(rare, graphIds(() -> g.V().has("phase", "rare").id().toList()));
        assertIndexed(executedSql());
        TimingExecuterListener.timing.clear();
        assertEquals(rare, graphIds(() -> g.V().has("phase", P.within("rare", "unused")).id().toList()));
        assertIndexed(executedSql());
    }

    @Test
    public void unknownValuesMatchNothingInsteadOfFailingTheCast() {
        assertEquals(0L, (long) g.V().has("phase", "no-such-label").count().next());
        assertEquals(ROWS / 1000, (long) g.V().has("phase", P.within("rare", "no-such-label")).count().next());
        assertEquals(0L, (long) g.V().has("ref", "not-a-uuid").count().next());
        assertEquals(0L, (long) g.V().has("ref", ref7.toString().toUpperCase()).count().next());
    }

    @Test
    public void negationsStillMatchTheTextComparison() throws Exception {
        assertEquals(idsWhere("ref::text <> '" + ref7 + "'").size(), (long) g.V().has("ref", P.neq(ref7)).count().next());
        assertEquals(idsWhere("phase::text not in ('common')").size(), (long) g.V().has("phase", P.without("common")).count().next());
        assertEquals(idsWhere("mood::text = 'calm'").size(), (long) g.V().has("mood", "calm").count().next());
    }
}
