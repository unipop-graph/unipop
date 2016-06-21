package org.unipop.schema;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;

public interface EdgeSchema extends ElementSchema<Edge> {
    VertexSchema getOutVertexSchema();
    VertexSchema getInVertexSchema();

    PredicatesHolder toPredicates(PredicatesHolder predicates, List<Vertex> vertices, Direction direction);
}
