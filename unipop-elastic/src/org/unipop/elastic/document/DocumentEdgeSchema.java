package org.unipop.elastic.document;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.VertexSchema;

import java.util.Collection;

public interface DocumentEdgeSchema extends DocumentSchema<Edge>, EdgeSchema {
    Search getSearch(SearchVertexQuery query);
    Search getReduce(ReduceVertexQuery query);
    Search getLocal(LocalQuery query);
    Collection<Pair<String, Element>> parseLocal(String result, LocalQuery query);
    VertexSchema getOutVertexSchema();
    VertexSchema getInVertexSchema();

    QueryBuilder getSearch(SearchVertexQuery query);
}
