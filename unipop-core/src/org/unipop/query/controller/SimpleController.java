package org.unipop.query.controller;

import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.VertexSchema;

import java.util.Set;

public interface SimpleController extends
        SearchQuery.SearchController,
        SearchVertexQuery.SearchVertexController,
        AddVertexQuery.AddVertexController,
        AddEdgeQuery.AddEdgeController,
        PropertyQuery.PropertyController,
        RemoveQuery.RemoveController,
        DeferredVertexQuery.DeferredVertexController {
    Set<? extends VertexSchema> getVertexSchemas();
    Set<? extends EdgeSchema> getEdgeSchemas();
}
