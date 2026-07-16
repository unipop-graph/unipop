package org.unipop.jdbc.schemas;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;

import java.util.*;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 6/13/2016
 */
public class RowEdgeSchema extends AbstractRowSchema<Edge> implements JdbcEdgeSchema
{
    protected VertexSchema inVertexSchema;
    protected VertexSchema outVertexSchema;

    public RowEdgeSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);

        this.inVertexSchema = createVertexSchema("inVertex");
        this.outVertexSchema = createVertexSchema("outVertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if(vertexConfiguration == null) return null;
        if(vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new RowVertexSchema(vertexConfiguration, table, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(outVertexSchema, inVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if(edgeProperties == null) return null;
        Vertex outVertex = outVertexSchema.createElement(fields);
        if(outVertex == null) return null;
        Vertex inVertex = inVertexSchema.createElement(fields);
        if(inVertex == null) return null;
        UniEdge uniEdge = new UniEdge(edgeProperties, outVertex, inVertex, this, graph);
        return Collections.singleton(uniEdge);
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> inFields = inVertexSchema.toFields(edge.inVertex());
        Map<String, Object> outFields = outVertexSchema.toFields(edge.outVertex());
        return ConversionUtils.merge(Lists.newArrayList(edgeFields, inFields, outFields), this::mergeFields, false);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = super.toFields(propertyKeys);
        Set<String> outFields = outVertexSchema.toFields(propertyKeys);
        fields.addAll(outFields);
        Set<String> inFields = inVertexSchema.toFields(propertyKeys);
        fields.addAll(inFields);
        return fields;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder);
    }

    @Override
    public PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        return PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }

    public static final String SRC_ALIAS = "__unipop_src";

    /** Thrown by getJoinSearch when a requested target order column is absent on this vertex
     *  schema. Per spec, order+limit must push together or not at all, so the controller must fall
     *  back the whole invocation to the deferred path rather than join a partial set of schemas. */
    public static final class OrderNotPushableException extends RuntimeException {}

    /**
     * Single-direction adjacency JOIN that hydrates + filters + bounds the target vertex.
     * directedDir is OUT or IN (never BOTH). Returns null to skip this (edge,vertex) pair when the
     * edge or target predicates aborted (that schema matches nothing — correct pruning). Throws
     * {@link OrderNotPushableException} when a requested order column is absent on this vertex
     * schema, so the controller falls back the whole invocation to the deferred path (order+limit
     * push together or not at all).
     */
    public Select getJoinSearch(SearchVertexQuery query, JdbcVertexSchema vertexSchema, Direction directedDir) {
        VertexSchema srcSchema = directedDir.equals(Direction.OUT) ? outVertexSchema : inVertexSchema;
        VertexSchema tgtSchema = directedDir.equals(Direction.OUT) ? inVertexSchema : outVertexSchema;
        String srcCol = srcSchema.getFieldByPropertyKey(T.id.getAccessor());
        String endpointCol = tgtSchema.getFieldByPropertyKey(T.id.getAccessor());
        AbstractRowSchema<?> vSchema = (AbstractRowSchema<?>) vertexSchema;
        String vIdCol = vSchema.getFieldByPropertyKey(T.id.getAccessor());

        PredicatesHolder tgtHolder = vertexSchema.toPredicates(query.getTargetPredicates());
        if (tgtHolder.isAborted()) return null;
        PredicatesHolder edgeHolder = this.toPredicates(query.getVertices(), directedDir, query.getPredicates());
        if (edgeHolder.isAborted()) return null;

        Condition edgeCond = this.buildTranslator("e").translate(edgeHolder);
        Condition tgtCond = vSchema.buildTranslator("v").translate(tgtHolder);

        Table<Record> e = table(name(getTable())).as("e");
        Table<Record> v = table(name(vertexSchema.getTable())).as("v");
        List<SelectFieldOrAsterisk> projection = Arrays.asList(v.asterisk(), field(name("e", srcCol)).as(SRC_ALIAS));

        SelectConditionStep<Record> where = DSL.select(projection)
                .from(e).join(v).on(field(name("e", endpointCol)).eq(field(name("v", vIdCol))))
                .where(edgeCond.and(tgtCond));

        List<SortField<Object>> sorts = new ArrayList<>();
        if (query.getTargetOrders() != null) {
            for (Pair<String, Order> o : query.getTargetOrders()) {
                String col = vSchema.getFieldByPropertyKey(o.getValue0());
                if (col == null) throw new OrderNotPushableException();
                Field<Object> f = field(name("v", col));
                sorts.add(o.getValue1().equals(Order.desc) ? f.desc() : f.asc());
            }
        }
        int lim = query.getTargetLimit() < 0 ? Integer.MAX_VALUE : query.getTargetLimit();
        return sorts.isEmpty() ? where.limit(lim) : where.orderBy(sorts).limit(lim);
    }

    /**
     * Parse a join row into a UniEdge under a uniform convention: out = source-shell (the query
     * vertex, id only), in = hydrated target neighbour -- regardless of directedDir (which only
     * chose src/target columns for getJoinSearch). The edge is flagged adjacencyJoinDirected so
     * UniGraphVertexStep maps it by source (out) only, letting both() run as two directed joins
     * without double-counting (see UniEdge.isAdjacencyJoinDirected).
     */
    public Edge fromJoinRow(Map<String, Object> row, JdbcVertexSchema vertexSchema, Direction directedDir) {
        Vertex target = vertexSchema.createElement(row);
        if (target == null) return null;
        Object srcId = row.get(SRC_ALIAS);
        // ponytail: must be a mutable map -- UniElement's ctor calls properties.remove(...) to
        // pull out ~id/~label, which throws UnsupportedOperationException against
        // Collections.singletonMap (its entrySet iterator refuses remove()).
        Map<String, Object> shellProps = new HashMap<>();
        shellProps.put(T.id.getAccessor(), srcId);
        Vertex source = new UniVertex(shellProps, null, graph);
        // ponytail: the empty props => random UniEdge id is load-bearing. both() returns the same
        // physical edge twice (one OUT-directed copy, one IN-directed copy); searchJoin's HashSet
        // dedups by id, so distinct random ids keep both copies alive. Projecting the real edge id
        // here would collapse them and make both() under-count. Keep random ids (or dedup by identity).
        return new UniEdge(new HashMap<>(), source, target, this, graph).asAdjacencyJoinDirected();
    }

    @Override
    public String toString() {
        return "RowEdgeSchema{" +
                "inVertexSchema=" + inVertexSchema +
                ", outVertexSchema=" + outVertexSchema +
                "} " + super.toString();
    }
}
