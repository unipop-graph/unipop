package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sbarzilay on 10/14/15.
 */
public class NestedEdgeMapping implements EdgeMapping {
    private final String edgeLabel;
    private final String externalVertexLabel;
    private final Direction direction;
    private final String externalVertexField;

    public NestedEdgeMapping(String edgeLabel, String externalVertexLabel, Direction direction, String externalVertexField) {
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.externalVertexField = externalVertexField;
    }

    @Override
    public String getExternalVertexField() {
        return externalVertexField;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    private Map<String, Object> getEdgeByInID(ArrayList<Map<String, Object>> edges, Object inId) {
        for (Map<String, Object> edge : edges) {
            if (edge.get("inId").equals(inId)) return edge;
        }
        return null;
    }

    @Override
    public Map<String, Object> getProperties(Map<String, Object> entries, Object id) {
        @SuppressWarnings("unchecked")
        Map<String, Object> edge = getEdgeByInID((ArrayList<Map<String, Object>>) entries.get(externalVertexField), id);
        if (edge != null) {
            return edge;
        }
        return null;
    }

    @Override
    public String getLabel() {
        return edgeLabel;
    }

    @Override
    public String getExternalVertexLabel() {
        return externalVertexLabel;
    }

    @Override
    public Iterable<Object> getExternalVertexId(Map<String, Object> entries) {
        ArrayList<Object> ids = new ArrayList<>();
        @SuppressWarnings("unchecked")
        ArrayList<HashMap<String, Object>> edges = (ArrayList<HashMap<String, Object>>) entries.get(externalVertexField);
        if (edges != null) {
            edges.forEach(edge ->
                            ids.add(edge.get("inId"))
            );
        }
        return ids;
    }
}
