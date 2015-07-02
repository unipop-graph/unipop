package org.elasticgremlin.queryhandler.elasticsearch.virtualvertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.VertexHandler;
import org.elasticgremlin.structure.BaseVertex;
import org.elasticgremlin.structure.ElasticGraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VirtualVertexHandler implements VertexHandler {

    private static final long VERTEX_BULK = 1000;

    private ElasticGraph graph;
    private String label;
    private List<Vertex> vertices;

    public VirtualVertexHandler(ElasticGraph graph, String label) {
        this.graph = graph;
        this.label = label;
        this.vertices = new ArrayList<>();
    }

    @Override
    public Iterator<Vertex> vertices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        ArrayList<Vertex> vertices = new ArrayList<>();
        for(Object id : vertexIds) {
            BaseVertex vertex = new VirtualVertex(id, label, graph, null);
            vertices.add(vertex);
            vertex.setSiblings(vertices);
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        checkBulk();
        BaseVertex vertex = new VirtualVertex(vertexId, vertexLabel, graph, null);
        vertex.setSiblings(vertices);
        vertices.add(vertex);
        return vertex;
    }

    private void checkBulk() {
        if (vertices.size() >= VERTEX_BULK) {
            vertices = new ArrayList<>();
        }
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        throw new UnsupportedOperationException();
    }
}
