package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import java.util.Map;

public interface EdgeMapping {

    String getExternalVertexField();

    Direction getDirection();

    Object[] getProperties(Map<String, Object> entries);

    String getLabel() ;

    String getExternalVertexLabel() ;

    Object getExternalVertexId(Map<String, Object> entries);
}
