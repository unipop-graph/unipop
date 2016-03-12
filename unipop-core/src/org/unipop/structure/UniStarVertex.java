package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.InnerEdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.ControllerManager;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 3/10/16.
 */
public class UniStarVertex extends UniVertex {
    protected Set<UniInnerEdge> innerEdges;
    private Set<InnerEdgeController> innerEdgeControllers;

    public UniStarVertex(Object id, String label, Map keyValues, ControllerManager manager, UniGraph graph, Set<InnerEdgeController> innerEdgeControllers) {
        super(id, label, keyValues, manager, graph);
        innerEdges = new HashSet<>();
        this.innerEdgeControllers = innerEdgeControllers;
    }

    public void removeInnerEdge(UniInnerEdge innerEdge) {
        innerEdges.remove(innerEdge);
    }

    public void addInnerEdge(UniInnerEdge innerEdge) {
        this.innerEdges.add(innerEdge);
    }

    public void update(boolean force) {
        getManager().update(this, force);
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && !innerEdgeControllers.stream().map(InnerEdgeController::getEdgeLabel).collect(Collectors.toList()).contains(key);
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();
        innerEdges.stream().collect(Collectors.groupingBy(UniInnerEdge::getInnerEdgeController))
                .forEach((controller, edges) -> map.putAll(controller.allFields(edges)));
        return map;
    }

    @Override
    public void applyLazyFields(String label, Map properties) {
        HashMap<String, Object> clone = new HashMap<>(properties);
        innerEdgeControllers.stream().map(controller -> controller.parseEdges(this, clone)).flatMap(Collection::stream).forEach(this::addInnerEdge);
        super.applyLazyFields(label, clone);
    }

    public Set<BaseEdge> getInnerEdges(Predicates predicates) {
        return innerEdges.stream().filter(edge -> filterPredicates(edge, predicates)).collect(Collectors.toSet());
    }

    public Set<BaseEdge> getInnerEdges(Direction direction, List<String> edgeLabels, Predicates predicates) {
        return innerEdges.stream()
                .filter(edge -> filterPredicates(edge, predicates) &&
                        (edgeLabels.size() == 0 || edgeLabels.contains(edge.label())) &&
                        (direction.equals(Direction.BOTH) || direction.equals(this.equals(edge.outVertex()) ? Direction.OUT : Direction.IN)))
                .collect(Collectors.toSet());
    }

    private boolean filterPredicates(UniInnerEdge edge, Predicates predicates) {
        return predicates.hasContainers.stream().allMatch(predicate -> predicate.test(edge));
    }
}
