package org.elasticgremlin.queryhandler.virtualvertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.VertexHandler;
import org.elasticgremlin.structure.BaseVertex;
import org.elasticgremlin.structure.ElasticGraph;

import java.util.ArrayList;
import java.util.Iterator;

public class VirtualVertexHandler implements VertexHandler {

    private ElasticGraph graph;
    private String label;

    public VirtualVertexHandler(ElasticGraph graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Iterator<Vertex> vertices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        ArrayList<Vertex> vertices = new ArrayList<>();
        for(Object id : vertexIds)
            vertices.add(new VirtualVertex(id, label, graph, null));
        BaseVertex.setVertexSiblings(vertices);
        return vertices.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new VirtualVertex(vertexId, vertexLabel, graph, null);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        throw new UnsupportedOperationException();
    }
}
