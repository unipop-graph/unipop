package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;

import java.util.List;
import java.util.Map;

public interface ElasticEdgeSchema extends ElasticElementSchema<Edge> {
    Filter getFilter(List<Vertex> vertex, Predicates predicates);
    Edge createEdge(Map<String, Object> properties, Vertex outV, Vertex inV);
}
