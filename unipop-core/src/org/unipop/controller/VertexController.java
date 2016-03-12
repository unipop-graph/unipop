package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface VertexController {
    Iterator<BaseVertex> vertices(Predicates predicates);
    BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel);

    long vertexCount(Predicates predicates);

    Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal);

    BaseVertex addVertex(Object id, String label, Map<String, Object> properties);

    void init(Map<String, Object> conf, UniGraph graph) throws Exception;

    void addPropertyToVertex(BaseVertex vertex, BaseVertexProperty vertexProperty);

    void removePropertyFromVertex(BaseVertex vertex, Property property);

    void removeVertex(BaseVertex vertex);

    List<BaseElement> vertexProperties(List<BaseVertex> vertices);

    void update(BaseVertex vertex, boolean force);

    String getResource();

    void close();
}
