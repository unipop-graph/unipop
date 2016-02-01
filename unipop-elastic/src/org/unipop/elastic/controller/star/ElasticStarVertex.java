package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.star.inneredge.InnerEdge;
import org.unipop.elastic.controller.star.inneredge.InnerEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.helpers.ElasticLazyGetter;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ElasticStarVertex extends ElasticVertex<ElasticStarController> {
    private Set<InnerEdge> innerEdges;
    public String indexName;
    private Set<InnerEdgeController> innerEdgeControllers;

    public ElasticStarVertex(final Object id,
                             final String label,
                             Map<String, Object> keyValues,
                             UniGraph graph,
                             ElasticLazyGetter elasticLazyGetter,
                             ElasticStarController controller,
                             ElasticMutations elasticMutations,
                             String indexName,
                             Set<InnerEdgeController> innerEdgeControllers) {
        super(id, label, keyValues, controller, graph, elasticLazyGetter, elasticMutations, indexName);
        this.indexName = indexName;
        this.innerEdgeControllers = innerEdgeControllers;
        innerEdges = new HashSet<>();
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();
        innerEdges.stream().collect(Collectors.groupingBy(InnerEdge::getInnerEdgeController))
                .forEach((controller, edges) -> map.putAll(controller.allFields(edges)));
        return map;
    }

    public void addInnerEdge(InnerEdge innerEdge) {
        this.innerEdges.add(innerEdge);
    }

    public void removeInnerEdge(InnerEdge edge) {
        innerEdges.remove(edge);
    }

    public void update(boolean force) {
        if(removed) return;
        checkLazy();
        try {
            if(force) {
                elasticMutations.deleteElement(this, indexName, null);
                elasticMutations.addElement(this, indexName, null, false);
            }
            else elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void applyLazyFields(String label, Map<String, Object> properties) {
        HashMap<String, Object> clone = new HashMap<>(properties);
        innerEdgeControllers.stream().map(controller -> controller.parseEdges(this, clone)).flatMap(Collection::stream).forEach(this::addInnerEdge);
        super.applyLazyFields(label, clone);
    }

    public Set<BaseEdge> getInnerEdges(Predicates predicates) {
        checkLazy();
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
