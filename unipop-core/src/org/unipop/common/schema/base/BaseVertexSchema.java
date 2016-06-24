package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.common.schema.VertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.property.PropertySchema;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseVertexSchema extends BaseElementSchema<Vertex> implements VertexSchema {
    public BaseVertexSchema(List<PropertySchema> properties, UniGraph graph) {
        super(properties, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new UniVertex(properties, graph);
    }

    @Override
    public Set<String> getIdsAndLabelsKeys() {
        return Stream.of(getFieldNames(T.id.getAccessor()), getFieldNames(T.label.getAccessor())).flatMap(Collection::stream).collect(Collectors.toSet());
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
    public PredicatesHolder toPredicates(List<? extends Vertex> vertices) {
        HashSet<PredicatesHolder> predicates = new HashSet<>();
        vertices.stream().collect(Collectors.groupingBy(Vertex::label)).forEach((label, labelVertices) -> {
            PredicatesHolder labelPredicates = toPredicates(label, labelVertices);
            predicates.add(labelPredicates);
        });
        return PredicatesHolderFactory.or(predicates);
    }

    private PredicatesHolder toPredicates(String label, List<? extends Vertex> labelVertices) {
        HasContainer labelPredicate = new HasContainer(T.label.getAccessor(), P.eq(label));
        HasContainer ids = new HasContainer(T.id.getAccessor(), P.within(labelVertices.stream().map(Vertex::id).collect(Collectors.toList())));

        PredicatesHolder labelPredicates = PredicatesHolderFactory.and(ids, labelPredicate);
        return this.toPredicates(labelPredicates);
    }
}
