package org.unipop.elastic.document;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;

public interface DocumentEdgeSchema extends DocumentSchema<Edge>, EdgeSchema {
    QueryBuilder getSearch(SearchVertexQuery query);
}
