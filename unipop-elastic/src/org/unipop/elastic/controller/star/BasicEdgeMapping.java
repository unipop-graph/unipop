package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.lang.Object;import java.lang.Override;import java.lang.String;import java.util.Map;

public class BasicEdgeMapping implements EdgeMapping {
    private final String edgeLabel;
    private final String externalVertexLabel;
    private final Direction direction;
    private final String externalVertexField;

    public BasicEdgeMapping(String edgeLabel, String externalVertexLabel, Direction direction, String externalVertexField) {
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

    @Override
    public Object[] getProperties(Map<String, Object> entries) {
        return new Object[0];
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
    public Object getExternalVertexId(Map<String, Object> entries) {
        return entries.get(externalVertexField);
    }
}
