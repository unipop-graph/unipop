package org.unipop.schema.element;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;

public interface EdgeSchema extends ElementSchema<Edge> {
    PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates);
}
