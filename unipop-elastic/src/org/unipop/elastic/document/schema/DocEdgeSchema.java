package org.unipop.elastic.document.schema;

import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.EdgeSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class DocEdgeSchema extends DocSchema<Edge> implements EdgeSchema {
    private final VertexSchema outVertexSchema;
    private final VertexSchema inVertexSchema;

    public DocEdgeSchema(String index, String type, VertexSchema outVertexSchema, VertexSchema inVertexSchema, List<PropertySchema> properties, UniGraph graph) {
        super(index, type, properties, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    public VertexSchema getInVertexSchema() {
        return inVertexSchema;
    }

    @Override
    public VertexSchema getOutVertexSchema() {
        return outVertexSchema;
    }

    public Search toPredicates(SearchVertexQuery query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getPredicates(), query.gertVertices(), query.getDirection());
        return getSearch(query, predicatesHolder);
    }
}
