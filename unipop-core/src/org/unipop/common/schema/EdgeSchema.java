package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;
import java.util.Set;

public interface EdgeSchema extends ElementSchema<Edge> {
    VertexSchema getOutVertexSchema();
    VertexSchema getInVertexSchema();
    Set<String> getFieldNames(String key);

    PredicatesHolder toPredicates(PredicatesHolder predicates, List<Vertex> vertices, Direction direction);
}
