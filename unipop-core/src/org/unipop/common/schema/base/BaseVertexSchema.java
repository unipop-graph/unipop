package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BaseVertexSchema extends BaseElementSchema<Vertex> implements VertexSchema {
    public BaseVertexSchema(Map<String, PropertySchema> properties, PropertySchema dynamicProperties, UniGraph graph) {
        super(properties, dynamicProperties, graph);
    }

    @Override
    public Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new UniVertex(properties, graph);
    }

    @Override
    public PredicatesHolder toPredicates(List<? extends Vertex> vertices) {
        PredicatesHolder predicatesHolder = new PredicatesHolder(PredicatesHolder.Clause.Or);

        vertices.stream().collect(Collectors.groupingBy(Vertex::label)).forEach((label, labelVertices) -> {
            PredicatesHolder labelPredicates = toPredicates(label, labelVertices);
            predicatesHolder.add(labelPredicates);
        });

        if(predicatesHolder.getChildren().size() == 1) return predicatesHolder.getChildren().stream().findFirst().get();
        if(predicatesHolder.isEmpty()) return null;
        return predicatesHolder;
    }

    private PredicatesHolder toPredicates(String label, List<? extends Vertex> labelVertices) {
        PredicatesHolder labelPredicates = new PredicatesHolder(PredicatesHolder.Clause.And);
        HasContainer labelPredicate = new HasContainer(T.label.toString(), P.eq(label));
        labelPredicates.add(labelPredicate);
        HasContainer ids = new HasContainer(T.id.toString(), P.within(labelVertices.stream().map(Vertex::id).collect(Collectors.toList())));
        labelPredicates.add(ids);
        return this.toPredicates(labelPredicates);
    }
}
