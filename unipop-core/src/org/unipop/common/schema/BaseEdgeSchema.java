package org.unipop.common.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class BaseEdgeSchema extends BaseElementSchema<Edge> {
    private final ElementSchema<Vertex> outVertexSchema;
    private final ElementSchema<Vertex> inVertexSchema;

    public BaseEdgeSchema(ElementSchema<Vertex> outVertexSchema, ElementSchema<Vertex> inVertexSchema, Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
        super(properties, dynamicProperties, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    public Edge fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        Vertex outVertex = outVertexSchema.fromFields(fields);
        Vertex inVertex = inVertexSchema.fromFields(fields);
        return new UniEdge(edgeProperties, outVertex, inVertex, graph);
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = super.toFields(edge);
        Map<String, Object> inFields = inVertexSchema.toFields(edge.outVertex());
        Map<String, Object> outFields = outVertexSchema.toFields(edge.inVertex());
        Map<String, Object> fields = mergeFields(edgeFields, inFields, outFields);
        return fields;
    }


    private Map<String, Object> mergeFields(Map<String, Object>... fieldsArray) {
    }
}
