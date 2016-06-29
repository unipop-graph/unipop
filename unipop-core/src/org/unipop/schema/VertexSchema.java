package org.unipop.schema;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniVertex;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface VertexSchema extends ElementSchema<Vertex> {
    @Override
    default Vertex fromFields(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if(properties == null) return null;
        return new UniVertex(properties, getGraph());
    }

    default PredicatesHolder toPredicates(List<? extends Vertex> vertices) {
        HashSet<PredicatesHolder> predicates = new HashSet<>();
        vertices.stream().collect(Collectors.groupingBy(Vertex::label)).forEach((label, labelVertices) -> {
            PredicatesHolder labelPredicates = toPredicates(label, labelVertices);
            predicates.add(labelPredicates);
        });
        return PredicatesHolderFactory.or(predicates);
    }

    default PredicatesHolder toPredicates(String label, List<? extends Vertex> labelVertices) {
        HasContainer labelPredicate = new HasContainer(T.label.getAccessor(), P.eq(label));
        HasContainer ids = new HasContainer(T.id.getAccessor(), P.within(labelVertices.stream().map(Vertex::id).collect(Collectors.toList())));

        PredicatesHolder labelPredicates = PredicatesHolderFactory.and(ids, labelPredicate);
        return toPredicates(labelPredicates);
    }
}
