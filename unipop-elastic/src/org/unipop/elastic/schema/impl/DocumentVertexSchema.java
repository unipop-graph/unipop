package org.unipop.elastic.schema.impl;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.structure.UniVertex;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class DocumentVertexSchema extends DocumentSchema<Vertex> implements org.unipop.elastic.schema.ElasticVertexSchema {

    public DocumentVertexSchema(HierarchicalConfiguration configuration, UniGraph graph) throws MissingArgumentException {
        super(configuration, graph);
    }

    @Override
    public Vertex createElement(Map properties) {

        return new UniVertex(properties, graph);
    }

    @Override
    public Vertex createVertex(AddVertexQuery query) {
        return new UniVertex(query.getProperties(), graph);
    }
}
