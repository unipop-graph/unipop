package org.unipop.jdbc.schemas;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.schemas.property.EnumColumnSchema;
import org.unipop.jdbc.schemas.property.JsonbColumnSchema;
import org.unipop.jdbc.utils.ContextManager;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.or;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 3/7/2016
 */
public abstract class AbstractRowSchema<E extends Element> extends AbstractElementSchema<E> implements JdbcSchema<E> {
    protected String table;

    public AbstractRowSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);
        this.table = configuration.optString("table");
    }

    @Override
    public String getTable() {
        return this.table;
    }

    @Override
    public Object getId(E element) {
        return element.id();
    }

    @Override
    public Query getInsertStatement(E element) {
        JdbcSchema.Row row = toRow(element);
        if (row == null) return null;

        // ON CONFLICT DO NOTHING (dialect-portable via jOOQ): inserting an already-present
        // primary key must be idempotent. A plain INSERT throws a duplicate-key error on
        // strict databases like PostgreSQL (H2 tolerated the re-insert pattern); updates to
        // existing rows go through the separate update path in RowController.
        return DSL.insertInto(table(getTable()),
                    CollectionUtils.collect(row.getFields().keySet(), DSL::field))
                    .values(row.getFields().values())
                    .onDuplicateKeyIgnore();
    }

    @Override
    public List<E> parseResults(List<Map<String, Object>> result, PredicateQuery query) {
        List<E> elements = result.stream()
                .flatMap(r -> {
                    Collection<E> tableElements = this.fromFields(r);
                    return Objects.nonNull(tableElements) ? this.fromFields(r).stream() : Stream.empty();
                })
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        return elements;
    }

    @Override
    public Select getSearch(SearchQuery<E> query, PredicatesHolder predicatesHolder) {
        if (predicatesHolder.isAborted()) {
            return null;
        }

        // The id column is VARCHAR; tell the translator so it stringifies numeric Gremlin ids
        // (PostgreSQL won't compare varchar = bigint the way H2 did).
        String idField = getFieldByPropertyKey(T.id.getAccessor());
        Set<String> idFields = idField == null ? Collections.emptySet() : Collections.singleton(idField);
        Set<String> jsonbColumns = getPropertySchemas().stream()
                .filter(s -> s instanceof JsonbColumnSchema)
                .map(s -> ((JsonbColumnSchema) s).getJsonbColumn())
                .collect(Collectors.toSet());
        Set<String> enumColumns = getPropertySchemas().stream()
                .filter(s -> s instanceof EnumColumnSchema)
                .map(s -> ((EnumColumnSchema) s).getEnumColumn())
                .collect(Collectors.toSet());
        Condition conditions = new JdbcPredicatesTranslator(idFields, enumColumns, jsonbColumns).translate(predicatesHolder);
        int finalLimit = query.getLimit() < 0 ? Integer.MAX_VALUE : query.getLimit();

        SelectConditionStep<Record> where = createSqlQuery(query.getPropertyKeys())
                .where(conditions);

        List<Pair<String, Order>> orders = query.getOrders();
        if (orders != null){
            List<SortField<Object>> orderValues = orders.stream().filter(order -> !order.getValue1().equals(Order.shuffle))
                    .filter(order -> getFieldByPropertyKey(order.getValue0()) != null)
                    .map(order -> order.getValue1().equals(Order.asc) ?
                            field(getFieldByPropertyKey(order.getValue0())).asc() :
                            field(getFieldByPropertyKey(order.getValue0())).desc()).collect(Collectors.toList());
            if (orderValues.size() > 0)
                return where.orderBy(orderValues).limit(finalLimit);
        }

        return where.limit(finalLimit);

    }


    private <E extends Element> SelectJoinStep<Record> createSqlQuery(Set<String> columnsToRetrieve) {
        if (columnsToRetrieve == null) {
            return DSL.select().from(this.getTable());

        }

        Set<String> props = this.toFields(columnsToRetrieve);
        return DSL
                .select(props.stream().map(DSL::field).collect(Collectors.toList()))
                .from(this.getTable());
    }

    @Override
    public String toString() {
        return "AbstractRowSchema{" +
                "table='" + table + '\'' +
                "} " + super.toString();
    }
}
