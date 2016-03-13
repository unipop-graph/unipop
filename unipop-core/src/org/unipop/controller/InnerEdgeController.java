package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniInnerEdge;
import org.unipop.structure.UniStarVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by sbarzilay on 3/10/16.
 */
public interface InnerEdgeController {
    UniInnerEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties);
    Set<UniInnerEdge> parseEdges(UniStarVertex vertex, Map<String, Object> keyValues);
    UniInnerEdge parseEdge(UniStarVertex vertex, Map<String, Object> keyValues);
    Object getFilter(ArrayList<HasContainer> hasContainers);
    Object getFilter(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates);
    Map<String, Object> allFields(List<UniInnerEdge> edges);
    String getEdgeLabel();
    Direction getDirection();
    boolean shouldAddProperty(String key);
    void init(Map<String, Object> conf) throws Exception;
}
