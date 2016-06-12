package org.unipop.jdbc.simple;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.Iterator;

/**
 * Created by GurRo on 6/12/2016.
 */
public class RowController implements SimpleController {
    protected DSLContext dslContext;
    protected UniGraph graph;
    protected String tableName;
    protected RecordMapper<Record, UniVertex> vertexMapper;

    public RowController(UniGraph graph) {
        this.graph = graph;
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), this.graph)
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        return null;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {

    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {

    }

    @Override
    public void fetchProperties(DeferredVertexQuery query) {

    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        return null;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        return null;
    }
}
