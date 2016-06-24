package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.List;
import java.util.Set;

public interface VertexSchema extends ElementSchema<Vertex> {
    PredicatesHolder toPredicates(List<? extends Vertex> vertices);
    Set<String> getFieldNames(String key);
}
