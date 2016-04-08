//package org.unipop.common.controller.virtualvertex;
//
//import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
//import org.apache.tinkerpop.gremlin.structure.*;
//import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
//import org.unipop.controller.UniQuery;
//import org.unipop.query.UniQueryController;
//import org.unipop.controller.VertexQueryController;
//import org.unipop.structure.*;
//
//import java.util.*;
//
//public class VirtualVertexController implements UniQueryController<Vertex>{
//    private static final long VERTEX_BULK = 1000;
//
//    private UniGraph graph;
//    private String label;
//
//    public VirtualVertexController(UniGraph graph, String label) {
//        this.graph = graph;
//        this.label = label;
//    }
//
//    public VirtualVertexController(){}
//
//
//    @Override
//    public Iterator<Vertex> query(UniQuery predicates, Class<Vertex> returnType) {
//        Optional<HasContainer> optionalId = predicates.hasContainers.stream().filter(has -> has.getKey().equals(T.id.getAccessor())).findAny();
//        if(!optionalId.isPresent()) return EmptyIterator.instance();
//        Object idValue = optionalId.getValue().getValue();
//        Iterable ids = idValue instanceof Iterable ? (Iterable) idValue : Collections.singleton(idValue);
//
//        Optional<HasContainer> optionalLabel = predicates.hasContainers.stream().filter(has -> has.getKey().equals(T.label.getAccessor())).findAny();
//        String label = optionalLabel.isPresent() ? optionalLabel.getValue().getValue().toString() : Vertex.DEFAULT_LABEL;
//
//        Set<Vertex> vertices = new HashSet<>();
//        ids.forEach(id-> {
//            UniVertex uniVertex = new UniVertex(id, label, null, this, graph);
//            vertices.add(uniVertex);
//        });
//        return vertices.iterator();
//    }
//
//    @Override
//    public void remove(Vertex element) {
//
//    }
//
//    @Override
//    public void addProperty(Vertex Element, Property property) {
//        throw new UnsupportedOperationException("VirtualVertices do not hold any properties.");
//    }
//
//    @Override
//    public void removeProperty(Vertex Element, Property property) {
//
//    }
//
//    @Override
//    public UniVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
//        UniVertex uniVertex = new UniVertex(vertexId, vertexLabel, null, this, graph);
//        return uniVertex;
//    }
//
//    @Override
//    public UniVertex addVertex(Object id, String label, Map<String, Object> properties) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
//        this.graph = graph;
//        this.label = conf.getValue("label").toString();
//    }
//
//    @Override
//    public void commit() {
//
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//}
