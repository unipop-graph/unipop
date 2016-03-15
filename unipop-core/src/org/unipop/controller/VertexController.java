package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface VertexController extends Controller {
    Iterator<BaseVertex> vertices(Predicates predicates);
    BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel);

    Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal);
    long vertexCount(Predicates predicates);

    BaseVertex addVertex(Object id, String label, Map<String, Object> properties);
    void removeVertex(BaseVertex vertex);

    void addPropertyToVertex(BaseVertex vertex, BaseVertexProperty vertexProperty);
    void removePropertyFromVertex(BaseVertex vertex, Property property);

    List<BaseElement> vertexProperties(List<BaseVertex> vertices);
}
