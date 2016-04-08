package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.UniQuery;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.search.SearchVertexQuery;

import java.util.List;
import java.util.Map;

public interface ElasticEdgeSchema extends ElasticElementSchema<Edge> {
    Filter getFilter(SearchVertexQuery query);
    Edge createEdge(AddEdgeQuery query);
}
