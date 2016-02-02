package org.unipop.elastic.controller.template.controller.star.inneredge.nested;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.template.controller.star.TemplateStarVertex;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdge;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdgeController;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateNestedEdgeController implements TemplateInnerEdgeController {
    private Direction direction;
    private String edgeLabel;
    private String externalIdField;
    private String externalLabelField;
    private String edgeIdField;

    @SuppressWarnings("unchecked")
    @Override
    public Set<TemplateInnerEdge> parseEdges(TemplateStarVertex vertex, Map<String, Object> keyValues) {
        Object nested = keyValues.get(edgeLabel);
        if (nested instanceof Map) {
            TemplateInnerEdge edge = parseEdge(vertex, (Map<String, Object>) nested);
            return Collections.singleton(edge);
        } else if (nested instanceof List) {
            List<Map<String, Object>> edgesMaps = (List<Map<String, Object>>) nested;
            return edgesMaps.stream().map(edgeMap -> parseEdge(vertex, edgeMap)).collect(Collectors.toSet());
        } else throw new IllegalArgumentException(nested.toString());
    }

    @Override
    public TemplateInnerEdge parseEdge(TemplateStarVertex vertex, Map<String, Object> keyValues) {
        BaseVertex externalVertex = vertex.getGraph().getControllerManager()
                .vertex(direction.opposite(), keyValues.get(externalIdField), keyValues.get(externalLabelField).toString());
        BaseVertex outV = direction.equals(Direction.OUT) ? vertex : externalVertex;
        BaseVertex inV = direction.equals(Direction.IN) ? vertex : externalVertex;

        keyValues.remove(externalIdField);
        keyValues.remove(externalLabelField);

        TemplateInnerEdge edge = new TemplateInnerEdge(keyValues.get(edgeIdField), edgeLabel, keyValues, outV, inV, vertex.getController(), vertex.getGraph());
    }

    @Override
    public void init(Map<String, Object> conf) throws Exception {

    }
}
