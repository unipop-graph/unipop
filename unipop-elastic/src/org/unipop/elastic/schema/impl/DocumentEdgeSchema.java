package org.unipop.elastic.schema.impl;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;
import org.unipop.elastic.schema.ElasticVertexSchema;
import org.unipop.elastic.schema.Filter;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class DocumentEdgeSchema extends DocumentSchema<Edge> implements org.unipop.elastic.schema.ElasticEdgeSchema {
    private final ElasticVertexSchema outVertexSchema;
    private final ElasticVertexSchema inVertexSchema;

    public DocumentEdgeSchema(ElasticVertexSchema outVertexSchema, ElasticVertexSchema inVertexSchema, HierarchicalConfiguration configuration, UniGraph graph) {
        super(configuration, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    public Edge createElement(Map<String, Object> properties) {

        Vertex outVertex = outVertexSchema.fromFields(properties);
        Vertex inVertex = inVertexSchema.fromFields(properties);
        return new BaseEdge(properties, outVertex, inVertex, graph);
    }

    @Override
    public Filter getFilter(List<Vertex> vertex, Predicates predicates) {
        return null;
    }

    @Override
    public Edge createEdge(Map<String, Object> properties, Vertex outV, Vertex inV) {
        return null;
    }
}
