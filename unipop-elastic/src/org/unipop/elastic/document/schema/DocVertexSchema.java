package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.schema.EdgeSchema;
import org.unipop.schema.ElementSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DocVertexSchema extends DocSchema<Vertex> implements VertexSchema {
    Set<ElementSchema> edgeSchemas = new HashSet<>();

    public DocVertexSchema(String index, String type, List<PropertySchema> properties, UniGraph graph) {
        super(index, type, properties, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return this.edgeSchemas;
    }

    public void add(EdgeSchema schema) {
        edgeSchemas.add(schema);
    }
}
