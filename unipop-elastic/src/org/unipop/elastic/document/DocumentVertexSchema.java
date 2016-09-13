package org.unipop.elastic.document;

import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.element.VertexSchema;

import java.util.List;

public interface DocumentVertexSchema extends DocumentSchema<Vertex>, VertexSchema {
    Search getSearch(DeferredVertexQuery query);
    PredicatesHolder getVertexPredicates(List<Vertex> vertices);
}
