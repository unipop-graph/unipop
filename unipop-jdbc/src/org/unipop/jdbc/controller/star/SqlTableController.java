package org.unipop.jdbc.controller.star;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class SqlTableController implements VertexController {

    private final DSLContext dslContext;
    private final UniGraph graph;
    private final Connection conn;
    private final String tableName;
    private final EdgeMapping[] mappings;
    private final VertexMapper vertexMapper;

    public SqlTableController(String tableName, UniGraph graph, Connection conn,  EdgeMapping... mappings) {
        this.graph = graph;
        this.conn = conn;
        this.tableName = tableName;
        this.mappings = mappings;
        dslContext = DSL.using(conn, SQLDialect.DEFAULT);
        vertexMapper = new VertexMapper();
    }

    public DSLContext getContext() {
        return dslContext;
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        return dslContext.select().from(tableName).where(field("id").in(ids)).fetch(vertexMapper).iterator();
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        predicates.hasContainers.forEach(hasContainer -> select.where(createCondition(hasContainer)));
        //select.limit((int)predicates.limitLow, predicates.limitHigh < Long.MAX_VALUE ? (int)(predicates.limitHigh - predicates.limitLow) : Integer.MAX_VALUE);
        return select.fetch(vertexMapper).iterator();
    }

    private Condition createCondition(HasContainer hasContainer) {
        String key = hasContainer.getKey();
        Object value = hasContainer.getValue();
        BiPredicate<?, ?> predicate = hasContainer.getBiPredicate();

        if(key.equals(T.label.getAccessor())) return DSL.trueCondition();

        if(key.equals("~id"))
            return field(T.id.toString()).in(value.getClass().isArray() ? (Object[])value : new Object[]{value});
        Field<Object> field = field(key);
        if (predicate instanceof Compare) {
            String predicateString = predicate.toString();
            switch (predicateString) {
                case ("eq"):
                    return field.eq(value);
                case ("neq"):
                    return field.notEqual(value);
                case ("gt"):
                    return field.greaterThan(value);
                case ("gte"):
                    return field.greaterOrEqual(value);
                case ("lt"):
                    return field.lessThan(value);
                case ("lte"):
                    return field.lessOrEqual(value);
                case("inside"):
                    List items =(List) value;
                    Object firstItem = items.get(0);
                    Object secondItem = items.get(1);
                    return field.between(firstItem, secondItem);
                default:
                    throw new IllegalArgumentException("predicate not supported in has step: " + predicate.toString());
            }
        } else if (predicate instanceof Contains) {
            if (predicate == Contains.without) return field.isNull();
            else if (predicate == Contains.within){
                if(value == null) return field.isNotNull();
               // else
            }
        }
        throw new IllegalArgumentException("predicate not supported by unipop: " + predicate.toString());
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return dslContext.select().from(tableName).where(field("id").eq(vertexId)).fetchOne(vertexMapper);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        properties.putIfAbsent("id", id);

        dslContext.insertInto(table(tableName), CollectionUtils.collect(properties.keySet(), DSL::field))
                .values(properties.values()).execute();

        return new SqlVertex(id, label, properties, this, graph);
    }

    private SqlTableController self = this;
    private class VertexMapper implements RecordMapper<Record, BaseVertex> {

        @Override
        public BaseVertex map(Record record) {
            Map<String, Object> stringObjectMap = record.intoMap();
            return new SqlVertex(stringObjectMap.get("ID"), tableName.toLowerCase(), stringObjectMap, self, graph);
        }
    }
}
