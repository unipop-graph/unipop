package org.unipop.schema.base;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.schema.EdgeSchema;
import org.unipop.schema.ElementSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.property.PropertySchema;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseEdgeSchema extends BaseElementSchema<Edge> implements EdgeSchema {
    private final VertexSchema outVertexSchema;
    private final VertexSchema inVertexSchema;

    public BaseEdgeSchema(VertexSchema outVertexSchema, VertexSchema inVertexSchema, List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    public Edge fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if(edgeProperties == null) return null;
        Vertex outVertex = outVertexSchema.fromFields(fields);
        if(outVertex == null) return null;
        Vertex inVertex = inVertexSchema.fromFields(fields);
        if(inVertex == null) return null;
        return new UniEdge(edgeProperties, outVertex, inVertex, graph);
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = super.toFields(edge);
        if(edgeFields == null) return null;
        Map<String, Object> inFields = inVertexSchema.toFields(edge.inVertex());
        Map<String, Object> outFields = outVertexSchema.toFields(edge.outVertex());
        edgeFields.putAll(inFields);
        edgeFields.putAll(outFields);
        return edgeFields;
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = new HashSet<>(super.toFields(propertyKeys));
        Set<String> outFields = outVertexSchema.toFields(propertyKeys);
        fields.addAll(outFields);
        Set<String> inFields = inVertexSchema.toFields(propertyKeys);
        fields.addAll(inFields);
        return fields;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicates, List<Vertex> vertices, Direction direction) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        return PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
    }

    private PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }

    @Override
    public Set<ElementSchema> getAllSchemas() {
        Set<ElementSchema> allSchemas = super.getAllSchemas();
        allSchemas.add(outVertexSchema);
        allSchemas.add(inVertexSchema);
        return allSchemas;
    }
}
