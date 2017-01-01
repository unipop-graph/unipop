package org.unipop.elastic.document;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.VertexSchema;

public interface DocumentVertexSchema extends DocumentSchema<Vertex>, VertexSchema {
    QueryBuilder getSearch(DeferredVertexQuery query);
}
