package org.unipop.jdbc.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.DSLContext;
import org.unipop.controller.Predicates;
import org.unipop.jdbc.controller.star.inneredge.InnerEdge;
import org.unipop.jdbc.controller.vertex.SqlVertex;
import org.unipop.jdbc.helpers.SqlLazyGetter;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 2/17/16.
 */
public class SqlStarVertex extends SqlVertex<SqlStarController>{
    private Set<InnerEdge> innerEdges;
    private Set<String> propertyNames;

    protected SqlStarVertex(Object id, String label, Map<String, Object> keyValues, SqlLazyGetter lazyGetter, String tableName, SqlStarController controller, UniGraph graph, Set<String> propertyNames) {
        super(id, label, keyValues, lazyGetter, tableName, controller, graph);
        innerEdges = new HashSet<>();
        this.propertyNames = propertyNames;
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && propertyNames.contains(key);
    }

    public void addInnerEdge(InnerEdge innerEdge){
        innerEdges.add(innerEdge);
    }

    public void removeInnerEdge(InnerEdge innerEdge){
        innerEdges.remove(innerEdge);
    }

    public Set<BaseEdge> getInnerEdges(Predicates predicates) {
        return innerEdges.stream().filter(edge -> filterPredicates(edge, predicates)).collect(Collectors.toSet());
    }

    public Set<BaseEdge> getInnerEdges(Direction direction, List<String> edgeLabels, Predicates predicates) {
        checkLazy();
        return innerEdges.stream()
                .filter(edge -> filterPredicates(edge, predicates) &&
                        (edgeLabels.size() == 0 || edgeLabels.contains(edge.label())) &&
                        (direction.equals(Direction.BOTH) || direction.equals(this.equals(edge.outVertex())? Direction.OUT : Direction.IN)))
                .collect(Collectors.toSet());
    }

    private boolean filterPredicates(InnerEdge edge, Predicates predicates) {
        checkLazy();
        return predicates.hasContainers.stream().allMatch(predicate -> predicate.test(edge));
    }
}
