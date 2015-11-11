package org.unipop.controller.virtualvertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;

public class VirtualVertexController implements VertexController {
    private static final long VERTEX_BULK = 1000;

    private UniGraph graph;
    private String label;

    public VirtualVertexController(UniGraph graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicatess, Metrics metrics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return new VirtualVertex(vertexId, vertexLabel, null, this, graph);
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
        throw new UnsupportedOperationException();
    }
}
