package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;

public interface ElasticVertexSchema extends ElasticElementSchema<Vertex> {
    Vertex createVertex(Map<String, Object> properties);
}
