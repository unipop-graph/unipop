package org.unipop.elastic.document;

import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;

public interface DocumentEdgeSchema extends DocumentSchema<Edge>, EdgeSchema {
    Search getSearch(SearchVertexQuery query);
}
