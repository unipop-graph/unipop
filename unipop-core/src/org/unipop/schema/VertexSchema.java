package org.unipop.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;

public interface VertexSchema extends ElementSchema<Vertex> {
    PredicatesHolder toPredicates(List<? extends Vertex> vertices);
}
