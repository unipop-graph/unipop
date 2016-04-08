package org.unipop.jdbc.controller.edge;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.ControllerProvider;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jooq.impl.DSL.table;

/**
 * Created by sbarzilay on 28/01/16.
 */
public class SqlEdgeController implements EdgeQueryController {

    protected DSLContext dslContext;
    protected UniGraph graph;
    protected String tableName;
    protected RecordMapper<Record, UniEdge> edgeMapper;
    protected String inIdField;
    protected String inLabelField;
    protected String outIdField;
    protected String outLabelField;
    protected String label;

    public SqlEdgeController(String tableName,
                             String inIdField,
                             String inLabelField,
                             String outIdField,
                             String outLabelField,
                             String label,
                             UniGraph graph,
                             Connection conn) {
        this.tableName = tableName;
        this.graph = graph;
        dslContext = DSL.using(conn, SQLDialect.SQLITE);
        edgeMapper = new EdgeMapper();
        this.inIdField = inIdField;
        this.inLabelField = inLabelField;
        this.outIdField = outIdField;
        this.outLabelField = outLabelField;
        this.label = label;
    }

    public SqlEdgeController(){}

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        this.tableName = conf.get("tableName").toString();
        this.graph = graph;
        Class.forName(conf.get("driverClass").toString());
        this.dslContext = DSL.using(
                DriverManager.getConnection(conf.get("connectionString").toString()), SQLDialect.SQLITE);
        this.edgeMapper = new EdgeMapper();
        this.inIdField = conf.get("inIdField").toString();
        this.inLabelField = conf.get("inLabelField").toString();
        this.outIdField = conf.get("outIdField").toString();
        this.outLabelField = conf.get("outLabelField").toString();
        this.label = conf.get("label").toString();
    }

    @Override
    public void close() {
        dslContext.close();
    }


    public DSLContext getContext() {
        return dslContext;
    }

    @Override
    public Iterator<UniEdge> edges(UniQuery uniQuery) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        uniQuery.hasContainers.forEach(hasContainer -> select.where(JooqHelper.createCondition(hasContainer)));
        select.limit(0, uniQuery.limitHigh < Long.MAX_VALUE ? (int) uniQuery.limitHigh : Integer.MAX_VALUE);
        return select.fetch(edgeMapper).iterator();
    }

    private SelectJoinStep<Record> createSelect(Vertex[] vertices, UniQuery uniQuery, String idField, String labelField){
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        Set<Object> ids = Stream.of(vertices).map(Element::id).collect(Collectors.toSet());
        Set<String> labels = Stream.of(vertices).map(Element::label).collect(Collectors.toSet());
        ArrayList<HasContainer> hasContainers = new ArrayList<>();
        uniQuery.hasContainers.forEach(hasContainers::add);
        hasContainers.add(new HasContainer(idField, P.within(ids)));
        hasContainers.add(new HasContainer(labelField, P.within(labels)));
        hasContainers.forEach(has -> select.where(JooqHelper.createCondition(has)));
        return select;
    }

    @Override
    public Iterator<UniEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        SelectJoinStep<Record> select;
        if (direction.equals(Direction.OUT))
            select = createSelect(vertices, uniQuery, outIdField, outLabelField);
        else if (direction.equals(Direction.IN))
            select = createSelect(vertices, uniQuery, inIdField, inLabelField);
        else{
            select = createSelect(vertices, uniQuery, outIdField, outLabelField);
            select.union(createSelect(vertices, uniQuery, inIdField, inLabelField));
        }
        return select.fetch(edgeMapper).iterator();
    }

    @Override
    public long edgeCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public UniEdge addEdge(Object edgeId, String label, UniVertex outV, UniVertex inV, Map<String, Object> properties) {
        if (edgeId == null)
            edgeId = outV.id().toString() + outV.label() + inV.id().toString() + inV.label();
        properties.putIfAbsent("id", edgeId);
        properties.put(inIdField, inV.id());
        properties.put(inLabelField, inV.label());
        properties.put(outIdField, outV.id());
        properties.put(outLabelField, outV.label());

        dslContext.insertInto(table(tableName), CollectionUtils.collect(properties.keySet(), DSL::field))
                .values(properties.values()).execute();

        return new SqlEdge(edgeId, label, properties, outV, inV, this, graph);
    }

    private SqlEdgeController self = this;
    private class EdgeMapper implements RecordMapper<Record, UniEdge> {

        @Override
        public UniEdge map(Record record) {
            //Change keys to lower-case. TODO: make configurable mapping
            Map<String, Object> stringObjectMap = new HashMap<>();
            record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
            ControllerProvider manager = graph.getControllerManager();
            Vertex inV = manager.vertex(Direction.IN, stringObjectMap.get(inIdField), stringObjectMap.get(inLabelField).toString());
            Vertex outV = manager.vertex(Direction.OUT, stringObjectMap.get(outIdField), stringObjectMap.get(outLabelField).toString());
            stringObjectMap.remove(inIdField);
            stringObjectMap.remove(inLabelField);
            stringObjectMap.remove(outIdField);
            stringObjectMap.remove(outLabelField);
            SqlEdge edge = new SqlEdge(stringObjectMap.get("id"), label, stringObjectMap, outV, inV, self, graph);
            return edge;
        }
    }
}
