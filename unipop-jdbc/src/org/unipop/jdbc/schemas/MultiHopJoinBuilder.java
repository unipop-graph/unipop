package org.unipop.jdbc.schemas;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.MultiHopQuery;
import org.unipop.schema.catalog.PathHop;
import org.unipop.schema.catalog.PathPlan;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

/**
 * Builds a 2-hop adjacency JOIN for exact {@link RowEdgeSchema} path plans.
 * <ul>
 *   <li>Vertex-final ({@code out}/{@code in}): projects final vertex + mid + start id.</li>
 *   <li>Edge-final ({@code outE}/{@code inE}): projects final edge row + mid + start id (no final vertex join).</li>
 * </ul>
 */
public final class MultiHopJoinBuilder {

    public static final String MID_PREFIX = "__unipop_mid_";
    public static final String MID_PROP = "__unipop_mid";
    public static final String EDGE_PREFIX = "__unipop_e1_";

    private MultiHopJoinBuilder() {}

    /**
     * Vertex-final 2-hop join (final hop returns vertices).
     * @throws RowEdgeSchema.OrderNotPushableException when final order column missing
     */
    public static Select buildTwoHop(MultiHopQuery query, PathPlan plan) {
        return query.isReturnsVertex() ? buildTwoHopVertexFinal(query, plan) : buildTwoHopEdgeFinal(query, plan);
    }

    private static Select buildTwoHopVertexFinal(MultiHopQuery query, PathPlan plan) {
        Ctx ctx = ctx(query, plan, true);
        if (ctx == null) return null;

        PredicatesHolder tgtHolder = ctx.fin.toPredicates(query.getFinalTargetPredicates());
        if (tgtHolder.isAborted()) return null;
        Condition ct = ctx.finSchema.buildTranslator("v").translate(tgtHolder);

        Table<Record> tv = table(name(ctx.fin.getTable())).as("v");
        List<SelectFieldOrAsterisk> projection = new ArrayList<>();
        for (String col : schemaColumns(ctx.finSchema)) {
            projection.add(field(name("v", col)));
        }
        projection.add(field(name("e0", ctx.e0SrcCol)).as(RowEdgeSchema.SRC_ALIAS));
        for (String col : schemaColumns(ctx.midSchema)) {
            projection.add(field(name("m", col)).as(MID_PREFIX + col));
        }

        SelectConditionStep<Record> where = DSL.select(projection)
                .from(ctx.te0)
                .join(ctx.tm).on(field(name("e0", ctx.e0TgtCol)).eq(field(name("m", ctx.midId))))
                .join(ctx.te1).on(field(name("e1", ctx.e1SrcCol)).eq(field(name("m", ctx.midId))))
                .join(tv).on(field(name("e1", ctx.e1TgtCol)).eq(field(name("v", ctx.finId))))
                .where(ctx.c0.and(ctx.c1).and(ct));

        return orderLimit(where, query, ctx.finSchema, "v");
    }

    /**
     * Edge-final 2-hop join ({@code out().outE()} / {@code in().inE()}): no final vertex table join.
     * Projects e1 columns (prefixed) + mid + start id. Order/limit apply to edge columns.
     */
    private static Select buildTwoHopEdgeFinal(MultiHopQuery query, PathPlan plan) {
        Ctx ctx = ctx(query, plan, false);
        if (ctx == null) return null;

        // Edge-level has() on hop2 already in c1; finalTargetPredicates unused for edge return.
        List<SelectFieldOrAsterisk> projection = new ArrayList<>();
        // Edge row: id, label, props + endpoint id/label columns needed by fromFields
        for (String col : edgeRowColumns(ctx.e1)) {
            projection.add(field(name("e1", col)).as(EDGE_PREFIX + col));
        }
        projection.add(field(name("e0", ctx.e0SrcCol)).as(RowEdgeSchema.SRC_ALIAS));
        for (String col : schemaColumns(ctx.midSchema)) {
            projection.add(field(name("m", col)).as(MID_PREFIX + col));
        }

        SelectConditionStep<Record> where = DSL.select(projection)
                .from(ctx.te0)
                .join(ctx.tm).on(field(name("e0", ctx.e0TgtCol)).eq(field(name("m", ctx.midId))))
                .join(ctx.te1).on(field(name("e1", ctx.e1SrcCol)).eq(field(name("m", ctx.midId))))
                .where(ctx.c0.and(ctx.c1));

        return orderLimitOnEdge(where, query, ctx.e1, "e1");
    }

    private static class Ctx {
        RowEdgeSchema e0, e1;
        JdbcVertexSchema mid, fin;
        AbstractRowSchema<?> midSchema, finSchema;
        String e0SrcCol, e0TgtCol, e1SrcCol, e1TgtCol, midId, finId;
        Condition c0, c1;
        Table<Record> te0, tm, te1;
    }

    private static Ctx ctx(MultiHopQuery query, PathPlan plan, boolean needFinalVertex) {
        if (plan == null || plan.size() != 2) return null;
        PathHop h0 = plan.getHops().get(0);
        PathHop h1 = plan.getHops().get(1);
        if (h0.getEdgeSchema().getClass() != RowEdgeSchema.class
                || h1.getEdgeSchema().getClass() != RowEdgeSchema.class) return null;
        if (!(h0.getTargetVertexSchema() instanceof JdbcVertexSchema)) return null;
        if (needFinalVertex && !(h1.getTargetVertexSchema() instanceof JdbcVertexSchema)) return null;

        Ctx c = new Ctx();
        c.e0 = (RowEdgeSchema) h0.getEdgeSchema();
        c.e1 = (RowEdgeSchema) h1.getEdgeSchema();
        c.mid = (JdbcVertexSchema) h0.getTargetVertexSchema();
        c.fin = needFinalVertex ? (JdbcVertexSchema) h1.getTargetVertexSchema() : null;
        Direction d0 = h0.getDirection();
        Direction d1 = h1.getDirection();

        VertexSchema e0src = d0 == Direction.OUT ? c.e0.getOutVertexSchema() : c.e0.getInVertexSchema();
        VertexSchema e0tgt = d0 == Direction.OUT ? c.e0.getInVertexSchema() : c.e0.getOutVertexSchema();
        VertexSchema e1src = d1 == Direction.OUT ? c.e1.getOutVertexSchema() : c.e1.getInVertexSchema();
        VertexSchema e1tgt = d1 == Direction.OUT ? c.e1.getInVertexSchema() : c.e1.getOutVertexSchema();

        c.e0SrcCol = e0src.getFieldByPropertyKey(T.id.getAccessor());
        c.e0TgtCol = e0tgt.getFieldByPropertyKey(T.id.getAccessor());
        c.e1SrcCol = e1src.getFieldByPropertyKey(T.id.getAccessor());
        c.e1TgtCol = e1tgt.getFieldByPropertyKey(T.id.getAccessor());
        c.midSchema = (AbstractRowSchema<?>) c.mid;
        c.midId = c.midSchema.getFieldByPropertyKey(T.id.getAccessor());
        if (c.fin != null) {
            c.finSchema = (AbstractRowSchema<?>) c.fin;
            c.finId = c.finSchema.getFieldByPropertyKey(T.id.getAccessor());
        }

        MultiHopQuery.HopSpec hop0 = query.getHops().get(0);
        MultiHopQuery.HopSpec hop1 = query.getHops().get(1);
        PredicatesHolder edge0Holder = c.e0.toPredicates(query.getStarts(), d0, hop0.getEdgePredicates());
        if (edge0Holder.isAborted()) return null;
        PredicatesHolder edge1Holder = c.e1.toPredicates(hop1.getEdgePredicates());
        if (edge1Holder.isAborted()) return null;

        c.c0 = c.e0.buildTranslator("e0").translate(edge0Holder);
        c.c1 = c.e1.buildTranslator("e1").translate(edge1Holder);
        c.te0 = table(name(c.e0.getTable())).as("e0");
        c.tm = table(name(c.mid.getTable())).as("m");
        c.te1 = table(name(c.e1.getTable())).as("e1");
        return c;
    }

    private static Select orderLimit(SelectConditionStep<Record> where, MultiHopQuery query,
                                     AbstractRowSchema<?> orderSchema, String alias) {
        List<SortField<Object>> sorts = new ArrayList<>();
        if (query.getFinalOrders() != null) {
            for (Pair<String, Order> o : query.getFinalOrders()) {
                String col = orderSchema.getFieldByPropertyKey(o.getValue0());
                if (col == null) throw new RowEdgeSchema.OrderNotPushableException();
                Field<Object> f = field(name(alias, col));
                sorts.add(o.getValue1().equals(Order.desc) ? f.desc() : f.asc());
            }
        }
        int lim = query.getFinalLimit() < 0 ? Integer.MAX_VALUE : query.getFinalLimit();
        return sorts.isEmpty() ? where.limit(lim) : where.orderBy(sorts).limit(lim);
    }

    private static Select orderLimitOnEdge(SelectConditionStep<Record> where, MultiHopQuery query,
                                           RowEdgeSchema edge, String alias) {
        List<SortField<Object>> sorts = new ArrayList<>();
        if (query.getFinalOrders() != null) {
            for (Pair<String, Order> o : query.getFinalOrders()) {
                String col = edge.getFieldByPropertyKey(o.getValue0());
                if (col == null) throw new RowEdgeSchema.OrderNotPushableException();
                Field<Object> f = field(name(alias, col));
                sorts.add(o.getValue1().equals(Order.desc) ? f.desc() : f.asc());
            }
        }
        int lim = query.getFinalLimit() < 0 ? Integer.MAX_VALUE : query.getFinalLimit();
        return sorts.isEmpty() ? where.limit(lim) : where.orderBy(sorts).limit(lim);
    }

    private static Set<String> edgeRowColumns(RowEdgeSchema edge) {
        Set<String> cols = schemaColumns(edge);
        // Endpoint ref columns (out/in id/label fields) required by fromFields
        if (edge.getOutVertexSchema() != null) {
            String oid = edge.getOutVertexSchema().getFieldByPropertyKey(T.id.getAccessor());
            if (oid != null) cols.add(oid);
            String ol = edge.getOutVertexSchema().getFieldByPropertyKey(T.label.getAccessor());
            if (ol != null) cols.add(ol);
        }
        if (edge.getInVertexSchema() != null) {
            String iid = edge.getInVertexSchema().getFieldByPropertyKey(T.id.getAccessor());
            if (iid != null) cols.add(iid);
            String il = edge.getInVertexSchema().getFieldByPropertyKey(T.label.getAccessor());
            if (il != null) cols.add(il);
        }
        return cols;
    }

    private static Set<String> schemaColumns(AbstractRowSchema<?> schema) {
        Set<String> cols = new HashSet<>();
        String id = schema.getFieldByPropertyKey(T.id.getAccessor());
        if (id != null) cols.add(id);
        String label = schema.getFieldByPropertyKey(T.label.getAccessor());
        if (label != null) cols.add(label);
        for (PropertySchema p : schema.getPropertySchemas()) {
            if (p == null || p.getKey() == null) continue;
            String col = schema.getFieldByPropertyKey(p.getKey());
            if (col != null) cols.add(col);
        }
        if (cols.isEmpty() && id != null) cols.add(id);
        return cols;
    }

    /**
     * Vertex-final: adjacencyJoinDirected edge (out=start shell, in=final vertex) + mid property.
     */
    public static Edge fromTwoHopRow(Map<String, Object> row, JdbcVertexSchema midSchema,
                                     JdbcVertexSchema finalVertexSchema, UniGraph graph) {
        Map<String, Object> finalFields = new HashMap<>();
        Map<String, Object> midFields = new HashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            String k = e.getKey();
            if (k == null) continue;
            if (k.startsWith(MID_PREFIX)) {
                midFields.put(k.substring(MID_PREFIX.length()), e.getValue());
            } else if (!RowEdgeSchema.SRC_ALIAS.equals(k)) {
                finalFields.put(k, e.getValue());
            }
        }
        Vertex target = finalVertexSchema.createElement(finalFields);
        if (target == null) return null;
        Vertex mid = midSchema.createElement(midFields);
        Object srcId = row.get(RowEdgeSchema.SRC_ALIAS);
        Map<String, Object> shellProps = new HashMap<>();
        shellProps.put(T.id.getAccessor(), srcId);
        Vertex source = new UniVertex(shellProps, null, graph);
        UniEdge edge = new UniEdge(new HashMap<>(), source, target, null, graph).asAdjacencyJoinDirected();
        if (mid != null) {
            edge.property(MID_PROP, mid);
        }
        return edge;
    }

    /**
     * Edge-final: real edge from e1 columns + mid stashed for path; also stores start id on
     * {@link RowEdgeSchema#SRC_ALIAS} property for traverser remapping.
     */
    public static Edge fromTwoHopEdgeRow(Map<String, Object> row, RowEdgeSchema edgeSchema,
                                         JdbcVertexSchema midSchema, UniGraph graph) {
        Map<String, Object> edgeFields = new HashMap<>();
        Map<String, Object> midFields = new HashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            String k = e.getKey();
            if (k == null) continue;
            if (k.startsWith(MID_PREFIX)) {
                midFields.put(k.substring(MID_PREFIX.length()), e.getValue());
            } else if (k.startsWith(EDGE_PREFIX)) {
                edgeFields.put(k.substring(EDGE_PREFIX.length()), e.getValue());
            }
        }
        java.util.Collection<Edge> edges = edgeSchema.fromFields(edgeFields);
        if (edges == null || edges.isEmpty()) return null;
        Edge edge = edges.iterator().next();
        Vertex mid = midSchema.createElement(midFields);
        if (mid != null && edge instanceof UniEdge) {
            ((UniEdge) edge).property(MID_PROP, mid);
        }
        Object srcId = row.get(RowEdgeSchema.SRC_ALIAS);
        if (srcId != null && edge instanceof UniEdge) {
            ((UniEdge) edge).property(RowEdgeSchema.SRC_ALIAS, srcId);
        }
        return edge;
    }
}
