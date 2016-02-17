package org.unipop.jdbc.controller.star.inneredge;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.jdbc.controller.star.SqlStarVertex;
import org.unipop.structure.BaseVertex;

import java.util.Map;

/**
 * Created by sbarzilay on 2/17/16.
 */
public interface InnerEdgeController {
    InnerEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties);

    InnerEdge parseEdge(SqlStarVertex vertex, Map<String, Object> keyValues);

    String getEdgeLabel();

    Direction getDirection();

    void init(Map<String, Object> conf) throws Exception;
}
