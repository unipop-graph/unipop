package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.EdgeSchema;
import org.unipop.common.schema.VertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.property.PropertySchema;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseEdgeSchema extends BaseElementSchema<Edge> implements EdgeSchema {
    private final VertexSchema outVertexSchema;
    private final VertexSchema inVertexSchema;

    public BaseEdgeSchema(VertexSchema outVertexSchema, VertexSchema inVertexSchema, List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
        this.outVertexSchema = outVertexSchema;
        this.inVertexSchema = inVertexSchema;
    }

    @Override
    public Set<String> getFieldNames(String key) {
        Optional<PropertySchema> first = propertySchemas.stream().filter(propertySchema -> propertySchema.getKey().equals(key)).findFirst();
        if (first.isPresent()) {
            return first.get().getFields();
        }
        return null;
    }

    @Override
    public Set<String> getIdsAndLabelsKeys() {
        return Stream.of(getInVertexSchema().getFieldNames(T.id.getAccessor()),
                getInVertexSchema().getFieldNames(T.label.getAccessor()),
                getOutVertexSchema().getFieldNames(T.id.getAccessor()),
                getOutVertexSchema().getFieldNames(T.label.getAccessor()),
                getFieldNames(T.id.getAccessor()),
                getFieldNames(T.label.getAccessor())).flatMap(Collection::stream).collect(Collectors.toSet());
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
        Map<String, Object> inFields = inVertexSchema.toFields(edge.inVertex());
        edgeFields.putAll(inFields);
        Map<String, Object> outFields = outVertexSchema.toFields(edge.outVertex());
        edgeFields.putAll(outFields);
        return edgeFields;
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
        if (direction.equals(Direction.OUT)) return outPredicates;
        if (direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }

    @Override
    public VertexSchema getOutVertexSchema() {
        return this.outVertexSchema;
    }

    @Override
    public VertexSchema getInVertexSchema() {
        return this.inVertexSchema;
    }
}
