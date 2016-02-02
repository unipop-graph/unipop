package org.unipop.elastic.controller.template.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.template.controller.star.TemplateStarVertex;

import java.util.Map;
import java.util.Set;

/**
 * Created by sbarzilay on 02/02/16.
 */
public interface TemplateInnerEdgeController {
    Set<TemplateInnerEdge> parseEdges(TemplateStarVertex vertex, Map<String, Object> keyValues);

    TemplateInnerEdge parseEdge(TemplateStarVertex vertex, Map<String, Object> keyValues);

    Map<String, Object> getParams(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates);

    void init(Map<String, Object> conf) throws Exception;
}
