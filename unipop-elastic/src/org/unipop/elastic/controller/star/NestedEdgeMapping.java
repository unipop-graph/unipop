package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.helpers.ElasticHelper;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by sbarzilay on 10/14/15.
 */
public class NestedEdgeMapping implements EdgeMapping {
    private final String edgeLabel;
    private final List<String> externalVertexLabels;
    private final Direction direction;
    private final String externalVertexField;

    public NestedEdgeMapping(String edgeLabel, Direction direction, String externalVertexField, String... externalVertexLabels) {
        this.edgeLabel = edgeLabel;
        this.externalVertexLabels = Arrays.asList(externalVertexLabels);
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
    public List<String> getExternalVertexLabel() {
        return externalVertexLabels;
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

    @Override
    public FilterBuilder createFilter(Object[] ids) {
        Predicates p = new Predicates();
        p.hasContainers.add(new HasContainer(edgeLabel + "." + ElasticEdge.InId, P.within(ids)));
        return FilterBuilders.nestedFilter(externalVertexField, ElasticHelper.createFilterBuilder(p.hasContainers));
    }
}
