package org.unipop.jdbc.schemas;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jooq.impl.DSL.all;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 7/5/2016
 */
public class InnerRowEdgeSchema extends RowEdgeSchema {
    private final JdbcVertexSchema parentVertexSchema;
    private final VertexSchema childVertexSchema;


    public InnerRowEdgeSchema(JdbcVertexSchema parentVertexSchema, Direction parentDirection, JSONObject edgeJson, String table, UniGraph graph) {
        super(edgeJson, graph);
        this.table = table;
        this.parentVertexSchema = parentVertexSchema;
        this.childVertexSchema = createVertexSchema("vertex");

        this.outVertexSchema = parentDirection.equals(Direction.OUT) ? parentVertexSchema : childVertexSchema;
        this.inVertexSchema = parentDirection.equals(Direction.IN) ? parentVertexSchema : childVertexSchema;
    }

    @Override
    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if(vertexConfiguration == null) return null;
        if(vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new InnerRowVertexSchema(vertexConfiguration, table, graph);
    }

    @Override
    public Select getLocal(LocalQuery query, DSLContext dsl) {
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        PredicatesHolder edgePredicates = this.toPredicates(searchQuery.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), searchQuery.getDirection());
        if (edgePredicates.isAborted() || vertexPredicates.isAborted()) return null;
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        if (predicatesHolder.isAborted()) return null;
        // TODO: create select
        Set<String> outId = getOutVertexSchema().getPropertySchema(T.id.getAccessor()).toFields(Collections.emptySet());
        Set<String> id = getPropertySchema(T.id.getAccessor()).toFields(Collections.emptySet());
        List<Field<Object>> vertexFields = Stream.of(
                parentVertexSchema.toFields(searchQuery.getPropertyKeys()),
                childVertexSchema.toFields(searchQuery.getPropertyKeys())).flatMap(Collection::stream).map(DSL::field).collect(Collectors.toList());
        Field<Object>[] allFields = new Field[vertexFields.size() + 1];
        for (int i = 0; i < vertexFields.size(); i++) {
            allFields[i] = vertexFields.get(i);
        }
        allFields[vertexFields.size()] = (Field) DSL.rank().over(DSL.partitionBy(field(outId.iterator().next())).orderBy(field(id.iterator().next()))).as("r1");
        SelectGroupByStep select = ((SelectGroupByStep) createSelect(searchQuery, predicatesHolder, dsl, allFields));

        Set<String> fields = searchQuery.getPropertyKeys();
        if (fields == null)
            fields = this.getPropertySchemas().stream().map(PropertySchema::getKey).collect(Collectors.toSet());
        Set<String> props = this.toFields(fields);
        int limit = searchQuery.getLimit();
        int finalLimit = limit == -1 ? Integer.MAX_VALUE : limit + 1;
        List<Field<Object>> queryFields = props.stream().filter(p -> p != null).map(DSL::field).collect(Collectors.toList());
        queryFields = Stream.of(queryFields, Arrays.asList(allFields)).flatMap(Collection::stream).distinct().collect(Collectors.toList());
        SelectConditionStep<Record> selectf = dsl.select(queryFields).from(select).where(field("r1").lt(finalLimit));

        return selectf;
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(childVertexSchema);
    }

    @Override
    public String getTable() {
        return parentVertexSchema.getTable();
    }

    @Override
    public Select getSearch(SearchQuery<Edge> query, PredicatesHolder predicatesHolder, DSLContext context, Field... fields) {
        List<Field<Object>> vertexFields = Stream.of(parentVertexSchema.toFields(query.getPropertyKeys()),
                childVertexSchema.toFields(query.getPropertyKeys()))
                .flatMap(Collection::stream)
                .map(DSL::field).collect(Collectors.toList());
        Field[] allFields = new Field[fields.length + vertexFields.size()];
        for (int i = 0; i < fields.length; i++) {
            allFields[i] = fields[i];
        }
        for (int i = 0; i < vertexFields.size(); i++) {
            allFields[i + fields.length] = vertexFields.get(i);
        }
        SelectJoinStep search = (SelectJoinStep) super.getSearch(query, predicatesHolder, context, allFields);
        if (search == null) return null;
        return search.where(field(this.getFieldByPropertyKey(T.id.getAccessor())).isNotNull());
    }

    @Override
    public String toString() {
        return "InnerRowEdgeSchema{" +
                "childVertexSchema=" + childVertexSchema +
                "} " + super.toString();
    }

    @Override
    public Query getInsertStatement(Edge element) {
        JdbcSchema.Row row = toRow(element);
        if (row == null) return null;
        Map<Field<Object>, Object> fields = row.getFields().entrySet().stream()
                .collect(Collectors.toMap((entry) -> field(entry.getKey()), Map.Entry::getValue));
//        return DSL.update(table(getTable())).set(fields).where(field(this.getFieldByPropertyKey(T.id.getAccessor())).eq(row.getId()));
        return DSL.insertInto(table(getTable())).set(fields);
    }
}
