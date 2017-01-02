package org.unipop.jdbc.schemas;

import com.google.common.collect.Iterators;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.utils.ContextManager;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.schema.property.PropertySchema;
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

        return DSL.insertInto(table(getTable()),
                    CollectionUtils.collect(row.getFields().keySet(), DSL::field))
                    .values(row.getFields().values());
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
    public Select createSelect(SearchQuery<E> query, PredicatesHolder predicatesHolder, Field... fields) {
        if (predicatesHolder.isAborted()) {
            return null;
        }

        Condition conditions = new JdbcPredicatesTranslator().translate(predicatesHolder);
        int finalLimit = query.getLimit() < 0 ? Integer.MAX_VALUE : query.getLimit();

        SelectConditionStep<Record> where = createSqlQuery(query.getPropertyKeys(), fields)
                .where(conditions);

        List<Pair<String, Order>> orders = query.getOrders();
        if (orders != null){
            List<SortField<Object>> orderValues = orders.stream().filter(order -> !order.getValue1().equals(Order.shuffle))
                    .filter(order -> getFieldByPropertyKey(order.getValue0()) != null)
                    .map(order -> order.getValue1().equals(Order.incr) ?
                            field(getFieldByPropertyKey(order.getValue0())).asc() :
                            field(getFieldByPropertyKey(order.getValue0())).desc()).collect(Collectors.toList());
            if (orderValues.size() > 0)
                return where.orderBy(orderValues);
        }
        return where;
    }

    @Override
    public Select getSearch(SearchQuery<E> query, PredicatesHolder predicatesHolder, Field... fields) {
        if (predicatesHolder.isAborted())
            return null;
        int finalLimit = query.getLimit() < 0 ? Integer.MAX_VALUE : query.getLimit();
        Select select = createSelect(query, predicatesHolder, fields);
        return ((SelectOrderByStep) select).limit(finalLimit);
    }


    private <E extends Element> SelectJoinStep<Record> createSqlQuery(Set<String> columnsToRetrieve, Field... fields) {
        if (columnsToRetrieve == null) {
            columnsToRetrieve = this.getPropertySchemas().stream().map(PropertySchema::getKey).collect(Collectors.toSet());
        }

        Set<String> props = this.toFields(columnsToRetrieve);
        Iterator<Field<Object>> fieldIterator = props.stream().filter(p -> p!= null).map(DSL::field).iterator();
        List<Field<Object>> fieldList = IteratorUtils.asList(Iterators.concat(fieldIterator, Arrays.asList(fields).iterator()));
        fieldList = fieldList.stream().distinct().collect(Collectors.toList());
        return DSL
                .select(fieldList)
                .from(this.getTable());
    }

    @Override
    public String toString() {
        return "AbstractRowSchema{" +
                "table='" + table + '\'' +
                "} " + super.toString();
    }
}
