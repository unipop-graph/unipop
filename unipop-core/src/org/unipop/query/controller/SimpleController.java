package org.unipop.query.controller;

import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;

public interface SimpleController extends
        SearchQuery.SearchController,
        SearchVertexQuery.SearchVertexController,
        AddVertexQuery.AddVertexController,
        AddEdgeQuery.AddEdgeController,
        PropertyQuery.PropertyController,
        RemoveQuery.RemoveController,
        DeferredVertexQuery.DeferredVertexController {
}
