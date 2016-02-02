package org.unipop.elastic.controller.template.controller.star.inneredge.nested;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.template.controller.star.TemplateStarVertex;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdge;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdgeController;
import org.unipop.elastic.controller.template.helpers.TemplateHelper;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateNestedEdgeController implements TemplateInnerEdgeController {
    private Direction direction;
    private String edgeLabel;
    private String externalIdField;
    private String externalLabelField;
    private String edgeIdField;

    public TemplateNestedEdgeController(Direction direction, String edgeLabel, String externalIdField, String externalLabelField, String edgeIdField) {
        this.direction = direction;
        this.edgeLabel = edgeLabel;
        this.externalIdField = externalIdField;
        this.externalLabelField = externalLabelField;
        this.edgeIdField = edgeIdField;
    }

    @Override
    public void init(Map<String, Object> conf) throws Exception {
        this.edgeLabel = conf.get("edgeLabel").toString();
        this.externalLabelField = conf.get("externalLabelField").toString();
        this.direction = conf.getOrDefault("direction", "out").toString().toLowerCase().equals("out") ? Direction.OUT : Direction.IN;
        this.externalIdField = conf.getOrDefault("externalIdField", "externalId").toString();
        this.edgeIdField = conf.getOrDefault("edgeIdField", "edgeId").toString();
    }

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
        } else if(nested == null){
            return null;
        }
        else throw new IllegalArgumentException(nested.toString());
    }

    @Override
    public TemplateInnerEdge parseEdge(TemplateStarVertex vertex, Map<String, Object> keyValues) {
        BaseVertex externalVertex = vertex.getGraph().getControllerManager()
                .vertex(direction.opposite(), keyValues.get(externalIdField), keyValues.get(externalLabelField).toString());
        BaseVertex outV = direction.equals(Direction.OUT) ? vertex : externalVertex;
        BaseVertex inV = direction.equals(Direction.IN) ? vertex : externalVertex;

        keyValues.remove(externalIdField);
        keyValues.remove(externalLabelField);

        TemplateInnerEdge edge = new TemplateNestedEdge(keyValues.get(edgeIdField), edgeLabel, null, outV, inV, vertex.getController(), vertex.getGraph());
        keyValues.remove(edgeIdField);
        keyValues.forEach(edge::addPropertyLocal);
        vertex.addInneredge(edge);
        return edge;
    }

    @Override
    public Map<String, Object> getParams(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Map<String, Object> params = new HashMap<>();
        if (edgeLabels.length == 0 || Arrays.asList(edgeLabels).contains(edgeLabel)){
            Set<Object> ids = Stream.of(vertices).map(Element::id).collect(Collectors.toSet());
            Set<String> labels = Stream.of(vertices).map(Element::label).collect(Collectors.toSet());
            List<HasContainer> hasContainers = new ArrayList<>();
            predicates.hasContainers.forEach(has -> hasContainers.add(new HasContainer(edgeLabel + "." + has.getKey(), P.within(has.getValue()))));
            hasContainers.add(new HasContainer(edgeLabel + "." + externalIdField, P.within(ids)));
            hasContainers.add(new HasContainer(edgeLabel + "." + externalLabelField, P.within(labels)));
            TemplateHelper.createTemplateParams(hasContainers).forEach(params::put);
        }
        return params;
    }
}
