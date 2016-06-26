package org.unipop.elastic.document.schema;

import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.base.BaseEdgeSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class DocEdgeSchema extends BaseEdgeSchema implements DocSchema<Edge> {
    private String index;
    private String type;

    public DocEdgeSchema(String index, String type, VertexSchema outVertexSchema, VertexSchema inVertexSchema, List<PropertySchema> properties, UniGraph graph) {
        super(outVertexSchema, inVertexSchema, properties, graph);
        this.index = index;
        this.type = type;
    }

    @Override
    public String getIndex() {
        return this.index;
    }

    @Override
    public String getType() {
        return this.type;
    }


    public Search toPredicates(SearchVertexQuery query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getPredicates(), query.gertVertices(), query.getDirection());
        return createSearchSource(query, predicatesHolder);
    }
}
