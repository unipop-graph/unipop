package org.unipop.elastic.document.schema;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class NestedEdgeSchema extends DocEdgeSchema {
    private String path;

    public NestedEdgeSchema(String index, String type, String path, VertexSchema outVertexSchema, VertexSchema inVertexSchema, List<PropertySchema> properties, UniGraph graph) {
        super(index, type, outVertexSchema, inVertexSchema, properties, graph);
        this.path = path;
    }

    @Override
    public QueryBuilder createQueryBuilder(PredicatesHolder predicatesHolder) {
        QueryBuilder queryBuilder = super.createQueryBuilder(predicatesHolder);
        return QueryBuilders.nestedQuery(this.path, queryBuilder);
    }
}
