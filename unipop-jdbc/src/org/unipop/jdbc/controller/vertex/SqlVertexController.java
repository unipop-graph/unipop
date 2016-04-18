package org.unipop.jdbc.controller.vertex;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.query.UniQuery;
import org.unipop.jdbc.helpers.SqlLazyGetter;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class SqlVertexController implements VertexQueryController {

    protected DSLContext dslContext;
    protected UniGraph graph;
    protected String tableName;
    protected RecordMapper<Record, UniVertex> vertexMapper;
    protected int idCount = 10000;
    protected Map<Direction, SqlLazyGetter> lazyGetters;

    public SqlVertexController(String tableName, UniGraph graph, Connection conn) {
        this.graph = graph;
        this.tableName = tableName;
        dslContext = DSL.using(conn, SQLDialect.DEFAULT);
        vertexMapper = new VertexMapper();
        this.lazyGetters = new HashMap<>();
        //dslContext.settings().setRenderNameStyle(RenderNameStyle.AS_IS);
    }

    public SqlVertexController() {
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        this.graph = graph;
        this.tableName = conf.get("tableName").toString();
        Class.forName(conf.get("driverClass").toString());
        this.dslContext = DSL.using(
                DriverManager.getConnection(conf.get("connectionString").toString()), SQLDialect.DEFAULT);
        this.vertexMapper = new VertexMapper();
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public void addPropertyToVertex(UniVertex vertex, UniVertexProperty vertexProperty) {
        getContext().update(table(vertex.label()))
                .set(field(vertexProperty.key()), vertexProperty.value())
                .where(field("ID").eq(vertex.id()))
                .execute();
    }

    @Override
    public void removePropertyFromVertex(UniVertex vertex, Property property) {
        Object n = null;
        getContext().update(table(vertex.label()))
                .set(field(property.key()), n)
                .where(field("ID").equal(vertex.id()))
                .execute();
    }

    @Override
    public void removeVertex(UniVertex vertex) {
        getContext().delete(table(vertex.label()))
                .where(field("ID").equal(vertex.id()))
                .execute();
    }

    @Override
    public List<UniElement> vertexProperties(Iterator<UniVertex> vertices) {
        if (vertices.isEmpty())
            return new ArrayList<>();
        Map<String, List<UniVertex>> verticesByIds = vertices.stream().filter(vertex1 -> vertex1.label().equals(tableName)).filter(vertex -> !vertex.properties().hasNext())
                .collect(Collectors.groupingBy(vertex -> vertex.id().toString()));

        if (!verticesByIds.isEmpty()) {
            SelectJoinStep<Record> select = dslContext.select().from(tableName);
            select.where(JooqHelper.createCondition(new HasContainer(T.id.toString(), P.within(verticesByIds.keySet()))));
            select.fetch().forEach(record -> {
                Map<String, Object> stringObjectMap = new HashMap<>();
                record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
                List<UniVertex> vertexList = verticesByIds.get(stringObjectMap.get("id"));
                stringObjectMap.forEach((key, value) -> addProperty(vertexList, key, value));
            });
        }
        return vertices.stream().map(vertex -> ((UniElement) vertex)).collect(Collectors.toList());
    }

    protected void addProperty(List<UniVertex> vertices, String key, Object value) {
        vertices.forEach(vertex -> vertex.addPropertyLocal(key, value));
    }

    @Override
    public void update(UniVertex vertex, boolean force) {
        throw new NotImplementedException();
    }

    @Override
    public String getResource() {
        return tableName;
    }

    @Override
    public void close() {
        dslContext.close();
    }


    public DSLContext getContext() {
        return dslContext;
    }

    public SelectJoinStep<Record> createSelect(UniQuery uniQuery) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        uniQuery.hasContainers.forEach(hasContainer -> select.where(JooqHelper.createCondition(hasContainer)));
        select.limit(0, uniQuery.limitHigh < Long.MAX_VALUE ? (int) uniQuery.limitHigh : Integer.MAX_VALUE);
        return select;
    }

    @Override
    public Iterator<UniVertex> vertices(UniQuery uniQuery) {
        SelectJoinStep<Record> select = createSelect(uniQuery);
        try {
            return select.fetch(vertexMapper).iterator();
        } catch (Exception e) {
            return EmptyIterator.INSTANCE;
        }
    }

    protected SqlLazyGetter getLazyGetter(Direction direction) {
        SqlLazyGetter sqlLazyGetter = lazyGetters.get(direction);
        if (sqlLazyGetter == null || !sqlLazyGetter.canRegister()) {
            sqlLazyGetter = new SqlLazyGetter(dslContext);
            lazyGetters.put(direction,
                    sqlLazyGetter);
        }
        return sqlLazyGetter;
    }

    @Override
    public UniVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        //return dslContext.select().from(tableName).where(field("id").eq(vertexId)).fetchOne(vertexMapper);
        UniDelayedVertex uniVertex = new UniDelayedVertex(vertexId, vertexLabel, graph.getControllerManager(), graph);
        uniVertex.addTransientProperty(new TransientProperty(uniVertex, "resource", getResource()));
        return uniVertex;
    }

    @Override
    public long vertexCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public UniVertex addVertex(Object id, String label, Map<String, Object> properties) {
        if (id == null) id = idCount++; //TODO: make this smarter...
        properties.putIfAbsent("id", id);

        dslContext.insertInto(table(tableName), CollectionUtils.collect(properties.keySet(), DSL::field))
                .values(properties.values()).execute();

        return createVertex(id, label, properties);
    }

    protected UniVertex createVertex(Object id, String label, Map<String, Object> properties) {
        UniVertex uniVertex = new UniVertex(id, label, properties, graph.getControllerManager(), graph);
        uniVertex.addTransientProperty(new TransientProperty(uniVertex, "resource", getResource()));
        return uniVertex;
    }

    private SqlVertexController self = this;

    private class VertexMapper implements RecordMapper<Record, UniVertex> {

        @Override
        public UniVertex map(Record record) {
            //Change keys to lower-case. TODO: make configurable mapping
            Map<String, Object> stringObjectMap = new HashMap<>();
            record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
            UniVertex uniVertex = new UniVertex(stringObjectMap.get("id"), tableName.toLowerCase(), stringObjectMap, graph.getControllerManager(), graph);
            uniVertex.addTransientProperty(new TransientProperty(uniVertex, "resource", getResource()));
            return uniVertex;
        }
    }
}
