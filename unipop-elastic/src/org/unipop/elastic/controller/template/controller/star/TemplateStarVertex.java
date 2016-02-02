package org.unipop.elastic.controller.template.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.star.inneredge.InnerEdge;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdge;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertex;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateStarVertex extends TemplateVertex<TemplateStarController>{
    private Set<TemplateInnerEdge> innerEdges;

    protected TemplateStarVertex(Object id, String label, Map keyValues, TemplateStarController controller, UniGraph graph, ElasticMutations elasticMutations, String index) {
        super(id, label, keyValues, controller, graph, elasticMutations, index);
        innerEdges = new HashSet<>();
    }

    public void addInneredge(TemplateInnerEdge innerEdge){
        innerEdges.add(innerEdge);
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

    private boolean filterPredicates(TemplateInnerEdge edge, Predicates predicates) {
        return predicates.hasContainers.stream().allMatch(predicate -> predicate.test(edge));
    }
}
