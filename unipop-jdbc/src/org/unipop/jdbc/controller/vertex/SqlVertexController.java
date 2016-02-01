package org.unipop.jdbc.controller.vertex;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.jdbc.helpers.SqlLazyGetter;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.jooq.impl.DSL.table;

public class SqlVertexController implements VertexController {

    protected DSLContext dslContext;
    protected UniGraph graph;
    protected String tableName;
    protected RecordMapper<Record, BaseVertex> vertexMapper;
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

    public SqlVertexController(){}

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
    public void close() {
        dslContext.close();
    }


    public DSLContext getContext() {
        return dslContext;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        predicates.hasContainers.forEach(hasContainer -> select.where(JooqHelper.createCondition(hasContainer)));
        select.limit(0, predicates.limitHigh < Long.MAX_VALUE ? (int)predicates.limitHigh : Integer.MAX_VALUE);
        try {
            return select.fetch(vertexMapper).iterator();
        }
        catch (Exception e){
            return EmptyIterator.INSTANCE;
        }
    }

    private SqlLazyGetter getLazyGetter(Direction direction) {
        SqlLazyGetter sqlLazyGetter = lazyGetters.get(direction);
        if (sqlLazyGetter == null || !sqlLazyGetter.canRegister()) {
            sqlLazyGetter = new SqlLazyGetter(dslContext);
            lazyGetters.put(direction,
                    sqlLazyGetter);
        }
        return sqlLazyGetter;
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        //return dslContext.select().from(tableName).where(field("id").eq(vertexId)).fetchOne(vertexMapper);
        return new SqlVertex(vertexId, vertexLabel, null, getLazyGetter(direction), tableName, this, graph);
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        if(id == null) id = idCount++; //TODO: make this smarter...
        properties.putIfAbsent("id", id);

        dslContext.insertInto(table(tableName), CollectionUtils.collect(properties.keySet(), DSL::field))
                .values(properties.values()).execute();

        return new SqlVertex(id, label, properties, null, tableName, this, graph);
    }

    private SqlVertexController self = this;
    private class VertexMapper implements RecordMapper<Record, BaseVertex> {

        @Override
        public BaseVertex map(Record record) {
            //Change keys to lower-case. TODO: make configurable mapping
            Map<String, Object> stringObjectMap = new HashMap<>();
            record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
            return new SqlVertex(stringObjectMap.get("id"), tableName.toLowerCase(), stringObjectMap, null, tableName, self, graph);
        }
    }
}
