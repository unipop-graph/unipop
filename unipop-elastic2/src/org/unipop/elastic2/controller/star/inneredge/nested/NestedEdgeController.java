package org.unipop.elastic2.controller.star.inneredge.nested;

import org.apache.commons.collections4.SetUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.Predicates;
import org.unipop.elastic2.controller.star.ElasticStarVertex;
import org.unipop.elastic2.controller.star.inneredge.InnerEdge;
import org.unipop.elastic2.controller.star.inneredge.InnerEdgeController;
import org.unipop.elastic2.helpers.ElasticHelper;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.stream.Collectors;

public class NestedEdgeController implements InnerEdgeController {
    private String vertexLabel;
    private String edgeLabel;
    private String externalVertexLabel;
    private Direction direction;
    private String externalVertexIdField;
    private String edgeIdField;

    public NestedEdgeController(String vertexLabel, String edgeLabel, Direction direction, String externalVertexIdField, String externalVertexLabel, String edgeIdField) {
        this.vertexLabel = vertexLabel;
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.externalVertexIdField = externalVertexIdField;
        this.edgeIdField = edgeIdField;
    }

    @Override
    public void init(Map<String, Object> conf) throws Exception {
        this.vertexLabel = conf.get("vertexLabel").toString();
        this.edgeLabel = conf.get("edgeLabel").toString();
        this.externalVertexLabel = conf.get("externalVertexLabel").toString();
        this.direction = conf.getOrDefault("direction", "out").toString().toLowerCase().equals("out") ? Direction.OUT : Direction.IN;
        this.externalVertexIdField = conf.getOrDefault("externalVertexIdField", "externalId").toString();
        this.edgeIdField = conf.getOrDefault("edgeIdField", "edgeId").toString();
    }

    @Override
    public InnerEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        if (!label.equals(edgeLabel)) return null;
        ElasticStarVertex starVertex = (ElasticStarVertex) (direction.equals(Direction.IN) ? inV : outV);
        if (!starVertex.label().equals(vertexLabel)) return null;
        NestedEdge edge = new NestedEdge(starVertex, edgeId, edgeLabel, this, outV, inV);
        properties.forEach((key, value) -> edge.addPropertyLocal(key, value));
        starVertex.addInnerEdge(edge);
        starVertex.update(false);
        return edge;
    }

    @Override
    public Set<InnerEdge> parseEdges(ElasticStarVertex vertex, Map<String, Object> keyValues) {
        Object nested = keyValues.get(edgeLabel);
        if (nested == null) return SetUtils.emptySet();
        keyValues.remove(edgeLabel);
        if (nested instanceof Map) {
            InnerEdge edge = parseEdge(vertex, (Map<String, Object>) nested);
            return Collections.singleton(edge);
        } else if (nested instanceof List) {
            List<Map<String, Object>> edgesMaps = (List<Map<String, Object>>) nested;
            return edgesMaps.stream().map(edgeMap -> parseEdge(vertex, edgeMap)).collect(Collectors.toSet());
        } else throw new IllegalArgumentException(nested.toString());
    }

    private InnerEdge parseEdge(ElasticStarVertex vertex, Map<String, Object> keyValues) {
        Object externalVertexId = keyValues.get(externalVertexIdField);
        Object edgeId = keyValues.get(edgeIdField);
        BaseVertex externalVertex = vertex.getGraph().getControllerManager().vertex(direction.opposite(), externalVertexId, externalVertexLabel);
        BaseVertex outV = direction.equals(Direction.OUT) ? vertex : externalVertex;
        BaseVertex inV = direction.equals(Direction.IN) ? vertex : externalVertex;
        NestedEdge edge = new NestedEdge(vertex, edgeId, edgeLabel, this, outV, inV);
        keyValues.forEach((key, value) -> edge.addPropertyLocal(key, value));
        vertex.addInnerEdge(edge);
        return edge;
    }

    @Override
    public QueryBuilder getQuery(ArrayList<HasContainer> hasContainers) {
        ArrayList<HasContainer> transformed = new ArrayList<>();

        for (HasContainer has : hasContainers) {
            if (has.getKey().equals(T.label.getAccessor())) {
                Object value = has.getValue();
                if (!value.equals(edgeLabel) && (value instanceof List && !((List<String>) value).contains(edgeLabel)))
                    return null;
            } else if (has.getKey().equals(T.id.getAccessor()))
                transformed.add(new HasContainer(edgeLabel + "." + edgeIdField, has.getPredicate()));
            else transformed.add(new HasContainer(edgeLabel + "." + has.getKey(), has.getPredicate()));
        }

        if (transformed.size() == 0)
            return QueryBuilders.nestedQuery(edgeLabel, QueryBuilders.matchAllQuery());
        return QueryBuilders.nestedQuery(edgeLabel, ElasticHelper.createQueryBuilder(transformed));
    }

    @Override
    public QueryBuilder getQuery(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        if (!direction.opposite().equals(this.direction) && !direction.equals(Direction.BOTH)) return null;
        if (edgeLabels.length > 0 && !Arrays.asList(edgeLabels).contains(edgeLabel)) return null;

        ArrayList ids = new ArrayList();
        for (Vertex vertex : vertices) {
            if (vertex.label().equals(externalVertexLabel))
                ids.add(vertex.id());
        }

        if (ids.size() == 0) return null;

        ArrayList<HasContainer> hasContainers = (ArrayList<HasContainer>) predicates.hasContainers.clone();
        hasContainers.add(new HasContainer(externalVertexIdField, P.within(ids.toArray())));
        return getQuery(hasContainers);
    }

    @Override
    public Map<String, Object> allFields(List<InnerEdge> edges) {
        Map<String, Object>[] edgesMap = new Map[edges.size()];
        for (int i = 0; i < edges.size(); i++) {
            InnerEdge innerEdge = edges.get(i);
            Map<String, Object> fields = innerEdge.allFields();
            fields.put(externalVertexIdField, innerEdge.vertices(direction.opposite()).next().id());
            fields.put(edgeIdField, innerEdge.id());
            edgesMap[i] = fields;
        }
        return Collections.singletonMap(edgeLabel, edgesMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NestedEdgeController that = (NestedEdgeController) o;

        if (edgeIdField != null ? !edgeIdField.equals(that.edgeIdField) : that.edgeIdField != null) return false;
        if (vertexLabel != null ? !vertexLabel.equals(that.vertexLabel) : that.vertexLabel != null) return false;
        if (edgeLabel != null ? !edgeLabel.equals(that.edgeLabel) : that.edgeLabel != null) return false;
        if (externalVertexLabel != null ? !externalVertexLabel.equals(that.externalVertexLabel) : that.externalVertexLabel != null)
            return false;
        if (direction != that.direction) return false;
        return !(externalVertexIdField != null ? !externalVertexIdField.equals(that.externalVertexIdField) : that.externalVertexIdField != null);

    }

    @Override
    public int hashCode() {
        int result = vertexLabel != null ? vertexLabel.hashCode() : 0;
        result = 31 * result + (edgeLabel != null ? edgeLabel.hashCode() : 0);
        result = 31 * result + (externalVertexLabel != null ? externalVertexLabel.hashCode() : 0);
        result = 31 * result + (direction != null ? direction.hashCode() : 0);
        result = 31 * result + (externalVertexIdField != null ? externalVertexIdField.hashCode() : 0);
        return result;
    }

    public boolean shouldAddProperty(String key) {
        return !externalVertexIdField.equals(key) && !edgeIdField.equals(key);
    }
}
