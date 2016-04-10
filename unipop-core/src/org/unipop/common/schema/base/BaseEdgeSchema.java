package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.EdgeSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.schema.property.PropertySchema;
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
        if(direction.equals(Direction.OUT)) edgePredicates.add(this.outVertexSchema.toPredicates(vertices));
        else if(direction.equals(Direction.IN)) edgePredicates.add(this.inVertexSchema.toPredicates(vertices));
        else {
            PredicatesHolder vertexPredicates = new PredicatesHolder(PredicatesHolder.Clause.Or);
            vertexPredicates.add(this.outVertexSchema.toPredicates(vertices));
            vertexPredicates.add(this.inVertexSchema.toPredicates(vertices));
            edgePredicates.add(vertexPredicates);
        }
        return edgePredicates;
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
