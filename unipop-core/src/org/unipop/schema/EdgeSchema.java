package org.unipop.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface EdgeSchema extends ElementSchema<Edge> {

    @Override
    default Edge fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if(edgeProperties == null) return null;
        Vertex outVertex = getOutVertexSchema().fromFields(fields);
        if(outVertex == null) return null;
        Vertex inVertex = getInVertexSchema().fromFields(fields);
        if(inVertex == null) return null;
        return new UniEdge(edgeProperties, outVertex, inVertex, getGraph());
    }

    @Override
    default Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> inFields = getInVertexSchema().toFields(edge.inVertex());
        Map<String, Object> outFields = getOutVertexSchema().toFields(edge.outVertex());
        return ConversionUtils.merge(Lists.newArrayList(edgeFields, inFields, outFields), this::mergeFields, false);
    }

    @Override
    default Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = getPropertySchemas().stream().flatMap(propertySchema ->
                propertySchema.toFields(propertyKeys).stream()).collect(Collectors.toSet());
        Set<String> outFields = getOutVertexSchema().toFields(propertyKeys);
        fields.addAll(outFields);
        Set<String> inFields = getInVertexSchema().toFields(propertyKeys);
        fields.addAll(inFields);
        return fields;
    }

    default PredicatesHolder toPredicates(PredicatesHolder predicates, List<Vertex> vertices, Direction direction) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        return PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
    }

    default PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.getOutVertexSchema().toPredicates(vertices);
        PredicatesHolder inPredicates = this.getInVertexSchema().toPredicates(vertices);
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }

    @Override
    default Set<ElementSchema> getWithChildSchemas() {
        HashSet<ElementSchema> schemas = Sets.newHashSet(this);
        schemas.add(getOutVertexSchema());
        schemas.add(getInVertexSchema());
        return schemas;
    }

    VertexSchema getInVertexSchema();

    VertexSchema getOutVertexSchema();
}
