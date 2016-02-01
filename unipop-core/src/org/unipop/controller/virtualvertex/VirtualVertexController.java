package org.unipop.controller.virtualvertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;

public class VirtualVertexController implements VertexController {
    private static final long VERTEX_BULK = 1000;

    private UniGraph graph;
    private String label;

    public VirtualVertexController(UniGraph graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    public VirtualVertexController(){}

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicatess) {
        Optional<HasContainer> optionalId = predicatess.hasContainers.stream().filter(has -> has.getKey().equals(T.id.getAccessor())).findAny();
        if(!optionalId.isPresent()) return EmptyIterator.instance();
        Object idValue = optionalId.get().getValue();
        Iterable ids = idValue instanceof Iterable ? (Iterable) idValue : Collections.singleton(idValue);

        Optional<HasContainer> optionalLabel = predicatess.hasContainers.stream().filter(has -> has.getKey().equals(T.label.getAccessor())).findAny();
        String label = optionalLabel.isPresent() ? optionalLabel.get().getValue().toString() : Vertex.DEFAULT_LABEL;

        Set<BaseVertex> vertices = new HashSet<>();
        ids.forEach(id-> vertices.add(new VirtualVertex(id, label, null, this, graph)));
        return vertices.iterator();
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

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        this.graph = graph;
        this.label = conf.get("label").toString();
    }

    @Override
    public void close() {

    }
}
