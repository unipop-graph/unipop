package org.unipop.elastic.schema.impl;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.UniQuery;
import org.unipop.elastic.schema.ElasticVertexSchema;
import org.unipop.elastic.schema.Filter;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class DocumentEdgeSchema extends DocumentSchema<Edge> implements org.unipop.elastic.schema.ElasticEdgeSchema {
    private final ElasticVertexSchema outVertexSchema;
    private final ElasticVertexSchema inVertexSchema;

    public DocumentEdgeSchema(ElasticVertexSchema outVertexSchema, ElasticVertexSchema inVertexSchema, HierarchicalConfiguration configuration, UniGraph graph) throws MissingArgumentException {
        super(configuration, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    public Edge createElement(Map<String, Object> properties) {

        Vertex outVertex = outVertexSchema.fromFields(properties);
        Vertex inVertex = inVertexSchema.fromFields(properties);
        return new UniEdge(properties, outVertex, inVertex, graph);
    }

    @Override
    public Filter getFilter(SearchVertexQuery query) {
        Filter filter = super.getFilter(query);
        ElasticVertexSchema elasticVertexSchema = query.getDirection().equals(Direction.IN) ? inVertexSchema : outVertexSchema;
        elasticVertexSchema.
        return null;
    }

    @Override
    public Edge createEdge(AddEdgeQuery query) {
        return new UniEdge(query.getProperties(), query.getOutVertex(), query.getInVertex(), graph)
    }
}
