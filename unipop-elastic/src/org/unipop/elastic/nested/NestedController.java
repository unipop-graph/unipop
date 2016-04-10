package org.unipop.elastic.nested;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;

import java.util.Iterator;

public class NestedController implements SimpleController {
    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        return null;
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
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        return null;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        return null;
    }
}
