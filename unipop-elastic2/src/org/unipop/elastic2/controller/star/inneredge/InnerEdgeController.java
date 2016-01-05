package org.unipop.elastic2.controller.star.inneredge;


import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.controller.Predicates;
import org.unipop.elastic2.controller.star.ElasticStarVertex;
import org.unipop.structure.BaseVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface InnerEdgeController {
    InnerEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties);

    Set<InnerEdge> parseEdges(ElasticStarVertex vertex, Map<String, Object> keyValues);

    QueryBuilder getQuery(ArrayList<HasContainer> hasContainers);

    QueryBuilder getQuery(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates);

    Map<String, Object> allFields(List<InnerEdge> edges);
}
