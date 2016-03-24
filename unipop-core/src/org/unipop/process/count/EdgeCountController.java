package org.unipop.process.count;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;

public interface EdgeCountController extends CountController<Edge> {
    long count(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates);
}
