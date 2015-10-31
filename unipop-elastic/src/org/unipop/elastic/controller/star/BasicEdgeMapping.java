package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.unipop.controller.Predicates;
import org.unipop.elastic.helpers.ElasticHelper;

import java.lang.Object;import java.lang.Override;import java.lang.String;
import java.util.ArrayList;
import java.util.Map;

public class BasicEdgeMapping implements EdgeMapping {
    private final String edgeLabel;
    private final String externalVertexLabel;
    private final Direction direction;
    private final String externalVertexField;
    private final String propertiesField;

    public BasicEdgeMapping(String edgeLabel, String externalVertexLabel, Direction direction, String externalVertexField, String propertiesField) {
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.externalVertexField = externalVertexField;
        this.propertiesField = propertiesField;
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
    public Map<String, Object> getProperties(Map<String, Object> entries, Object id) {
        if(entries.containsKey(propertiesField)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> propsMap = (Map<String, Object>) entries.get(propertiesField);
            return propsMap;
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
        if (entries.containsKey(externalVertexField)) {
            return new ArrayList<Object>() {{
                add(entries.get(externalVertexField));
            }};
        }
        return new ArrayList<>();
    }
}
