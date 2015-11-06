package org.unipop.elastic.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.FilterBuilder;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.star.ElasticStarVertex;
import org.unipop.structure.BaseVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InnerEdgeController {
    InnerEdge createEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties);

    Set<InnerEdge> parseEdges(ElasticStarVertex vertex, Map<String, Object> keyValues);

    FilterBuilder getFilter(ArrayList<HasContainer> hasContainers);

    FilterBuilder getFilter(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates);

    void addEdgeFields(List<InnerEdge> edges, Map<String, Object> map);

    Direction getDirection();
}
