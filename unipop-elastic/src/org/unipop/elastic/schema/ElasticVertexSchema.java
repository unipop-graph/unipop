package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.mutation.AddVertexQuery;

public interface ElasticVertexSchema extends ElasticElementSchema<Vertex> {
    Vertex createVertex(AddVertexQuery query);
}
