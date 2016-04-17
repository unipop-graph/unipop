package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.EdgeSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

public class BaseEdgeSchema extends BaseElementSchema<Edge> implements EdgeSchema {
    private final VertexSchema outVertexSchema;
    private final VertexSchema inVertexSchema;

    public BaseEdgeSchema(VertexSchema outVertexSchema, VertexSchema inVertexSchema, Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
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
        edgeFields.putAll(inFields);
        Map<String, Object> outFields = outVertexSchema.toFields(edge.inVertex());
        edgeFields.putAll(outFields);
        return edgeFields;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicates, List<Vertex> vertices, Direction direction) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        if(edgePredicates == null) return null;
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        if(vertexPredicates == null) return null;
        edgePredicates.add(vertexPredicates);
        return edgePredicates;
    }

    private PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(inPredicates == null && outPredicates == null) return null;
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        if(outPredicates == null || outPredicates.isEmpty()) return inPredicates;
        if(inPredicates == null || inPredicates.isEmpty()) return outPredicates;

        PredicatesHolder orPredicate = new PredicatesHolder(PredicatesHolder.Clause.Or);
        orPredicate.add(inPredicates);
        orPredicate.add(outPredicates);
        return orPredicate;
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
