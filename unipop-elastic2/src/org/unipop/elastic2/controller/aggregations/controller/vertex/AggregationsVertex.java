package org.unipop.elastic2.controller.aggregations.controller.vertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.manager.ControllerProvider;
import org.unipop.controller.Predicates;
import org.unipop.elastic2.controller.aggregations.controller.edge.AggregationsEdge;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class AggregationsVertex extends BaseVertex{
    private ElasticMutations elasticMutations;
    private String index;
    Set<AggregationsEdge> innerEdges;

    protected AggregationsVertex(Object id, String label, Map<String, Object> keyValues, ControllerProvider manager, UniGraph graph, ElasticMutations elasticMutations, String index) {
        super(id, label, keyValues, manager, graph);
        this.elasticMutations = elasticMutations;
        this.index = index;
        this.innerEdges = new HashSet<>();
    }

    public void addInnerEdge(AggregationsEdge edge){
        innerEdges.add(edge);
    }

    public Set<BaseEdge> getInnerEdges(Predicates predicates) {
        return innerEdges.stream().filter(edge -> filterPredicates(edge, predicates)).collect(Collectors.toSet());
    }

    public Set<BaseEdge> getInnerEdges(Direction direction, List<String> edgeLabels, Predicates predicates) {
        return innerEdges.stream()
                .filter(edge -> filterPredicates(edge, predicates) &&
                        (edgeLabels.size() == 0 || edgeLabels.contains(edge.label())) &&
                        (direction.equals(Direction.BOTH) || direction.equals(this.equals(edge.outVertex())? Direction.OUT : Direction.IN)))
                .collect(Collectors.toSet());
    }

    private boolean filterPredicates(AggregationsEdge edge, Predicates predicates) {
        return predicates.hasContainers.stream().allMatch(predicate -> predicate.test(edge));
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && !key.equals("label");
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {

    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {

    }
}
