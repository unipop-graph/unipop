package org.unipop.schema.element;

import org.unipop.structure.UniEdge;
import org.unipop.structure.UniVertex;

import java.util.Collection;
import java.util.Map;

public interface InnerEdgeSchema {
    Collection<UniEdge> fromVertex(UniVertex uniVertex, Map<String, Object> fields);
}
