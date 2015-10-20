package org.unipop.jdbc.controller.star;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.util.Iterator;

import static org.jooq.impl.DSL.field;

public class SqlTableController implements VertexController, EdgeController {

    private final DSLContext dslContext;
    private final UniGraph graph;
    private final Connection conn;
    private final String tableName;
    private final EdgeMapping[] mappings;

    public SqlTableController(String tableName, UniGraph graph, Connection conn,  EdgeMapping... mappings) {
        this.graph = graph;
        this.conn = conn;
        this.tableName = tableName;
        this.mappings = mappings;
        dslContext = DSL.using(conn, SQLDialect.DEFAULT);
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        Result<Record> results = dslContext.select().from(tableName).where(field("ID").in(ids)).fetch();
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return null;
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return null;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return null;
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        return null;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        return null;
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return null;
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return null;
    }
}
