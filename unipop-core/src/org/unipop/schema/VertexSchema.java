package org.unipop.schema;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.stream.Collectors;

public interface VertexSchema extends ElementSchema<Vertex> {

    @Override
    default Collection<Vertex> fromFields(Map<String, Object> fields) {
        return Collections.singleton(createElement(fields));
    }

    default Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if(properties == null) return null;
        return new UniVertex(properties, getGraph());
    }

    default PredicatesHolder toPredicates(List<? extends Vertex> vertices) {
        HashSet<PredicatesHolder> predicates = new HashSet<>();
        vertices.stream().collect(Collectors.groupingBy(Vertex::label)).forEach((label, labelVertices) -> {
            HasContainer labelPredicate = new HasContainer(T.label.getAccessor(), P.eq(label));
            HasContainer ids = new HasContainer(T.id.getAccessor(), P.within(labelVertices.stream().map(Vertex::id).collect(Collectors.toList())));
            PredicatesHolder labelPredicates = PredicatesHolderFactory.and(ids, labelPredicate);
            PredicatesHolder toPredicates = toPredicates(labelPredicates);
            predicates.add(toPredicates);
        });
        return PredicatesHolderFactory.or(predicates);
    }
}
