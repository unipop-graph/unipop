package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.List;

public class NestedVertexSchema extends DocSchema<Vertex> implements VertexSchema {
    private String path;

    public NestedVertexSchema(String index, String type, String path, List<PropertySchema> properties, UniGraph graph) {
        super(index, type, properties, graph);
        this.path = path;
    }

    @Override
    public QueryBuilder createQueryBuilder(PredicatesHolder predicatesHolder) {
        QueryBuilder queryBuilder = super.createQueryBuilder(predicatesHolder);
        return QueryBuilders.nestedQuery(this.path, queryBuilder);
    }
}
