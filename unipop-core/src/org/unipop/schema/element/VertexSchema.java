package org.unipop.schema.element;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An element schema which represents a vertex
 */
public interface VertexSchema extends ElementSchema<Vertex> {

    @Override
    default Collection<Vertex> fromFields(Map<String, Object> fields) {
        Vertex element = createElement(fields);
        if(element == null) return null;
        return Collections.singleton(element);
    }

    /**
     * Creates an element from data source fields
     * @param fields Data source fields
     * @return A vertex
     */
    Vertex createElement(Map<String, Object> fields);

    /**
     * Receives a list of vertices and converts them to predicates
     * @param vertices A list of vertices
     * @return Predicate holder
     */
    default PredicatesHolder toPredicates(List<? extends Vertex> vertices) {
        if(vertices == null || vertices.size() == 0) return PredicatesHolderFactory.abort();
        HashSet<PredicatesHolder> predicates = new HashSet<>();
        vertices.stream().collect(Collectors.groupingBy(Vertex::label)).forEach((label, labelVertices) -> {
            HasContainer labelPredicate = new HasContainer(T.label.getAccessor(), P.eq(label));
            HasContainer ids = new HasContainer(T.id.getAccessor(),
                    P.within(labelVertices.stream().map(Vertex::id).collect(Collectors.toSet())));
            PredicatesHolder labelPredicates = PredicatesHolderFactory.and(ids, labelPredicate);
            PredicatesHolder toPredicates = toPredicates(labelPredicates);
            predicates.add(toPredicates);
        });
        return PredicatesHolderFactory.or(predicates);
    }
}
