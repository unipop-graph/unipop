package org.elasticgremlin.queryhandler.stardoc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import java.util.Map;

public interface EdgeMapping {

    public String getExternalVertexField();

    public Direction getDirection();

    public Object[] getProperties(Map<String, Object> entries);

    public String getLabel() ;

    public String getExternalVertexLabel() ;

    public Object getExternalVertexId(Map<String, Object> entries);
}
