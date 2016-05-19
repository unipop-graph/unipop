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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public PredicatesHolder toPredicates(List<? extends Vertex> vertices) {
        HashSet<PredicatesHolder> predicates = new HashSet<>();
        vertices.stream().collect(Collectors.groupingBy(Vertex::label)).forEach((label, labelVertices) -> {
            PredicatesHolder labelPredicates = toPredicates(label, labelVertices);
            predicates.add(labelPredicates);
        });
        return PredicatesHolderFactory.or(predicates);
    }

    private PredicatesHolder toPredicates(String label, List<? extends Vertex> labelVertices) {
        HasContainer labelPredicate = new HasContainer(T.label.toString(), P.eq(label));
        HasContainer ids = new HasContainer(T.id.toString(), P.within(labelVertices.stream().map(Vertex::id).collect(Collectors.toList())));

        PredicatesHolder labelPredicates = PredicatesHolderFactory.and(ids, labelPredicate);
        return this.toPredicates(labelPredicates);
    }
}
