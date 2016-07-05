package org.unipop.jdbc.schemas;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;

/**
 * @author Gur Ronen
 * @since 6/13/2016
 */
public class RowEdgeSchema extends AbstractRowSchema<Edge> implements JdbcEdgeSchema
{
    private final VertexSchema inVertexSchema;
    private final VertexSchema outVertexSchema;

    public RowEdgeSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);

        this.inVertexSchema = createVertexSchema("inVertex", graph);
        this.outVertexSchema = createVertexSchema("outVertex", graph);
    }

    private VertexSchema createVertexSchema(String key, UniGraph graph) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if(vertexConfiguration == null) return null;
        if(vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new RowVertexSchema(vertexConfiguration, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(outVertexSchema, inVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if(edgeProperties == null) return null;
        Vertex outVertex = outVertexSchema.createElement(fields);
        if(outVertex == null) return null;
        Vertex inVertex = inVertexSchema.createElement(fields);
        if(inVertex == null) return null;
        UniEdge uniEdge = new UniEdge(edgeProperties, outVertex, inVertex, graph);
        return Collections.singleton(uniEdge);
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> inFields = inVertexSchema.toFields(edge.inVertex());
        Map<String, Object> outFields = outVertexSchema.toFields(edge.outVertex());
        return ConversionUtils.merge(Lists.newArrayList(edgeFields, inFields, outFields), this::mergeFields, false);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = super.toFields(propertyKeys);
        Set<String> outFields = outVertexSchema.toFields(propertyKeys);
        fields.addAll(outFields);
        Set<String> inFields = inVertexSchema.toFields(propertyKeys);
        fields.addAll(inFields);
        return fields;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder);
    }

    @Override
    public PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        return PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }

    @Override
    public String toString() {
        return "RowEdgeSchema{" +
                "inVertexSchema=" + inVertexSchema +
                ", outVertexSchema=" + outVertexSchema +
                "} " + super.toString();
    }
}
