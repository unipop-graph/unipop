package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;

public interface ElasticVertexSchema extends ElasticElementSchema {

    Vertex createVertex(Object id, String label, Map<String, Object> properties);
    Filter getFilter(Object id, String label);
}
