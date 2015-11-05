package org.unipop.elastic.controller.star.inneredge.nested;

import org.apache.commons.collections4.SetUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.star.ElasticStarVertex;
import org.unipop.elastic.controller.star.inneredge.InnerEdge;
import org.unipop.elastic.controller.star.inneredge.InnerEdgeController;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.stream.Collectors;

public class NestedEdgeController implements InnerEdgeController {
    private String vertexLabel;
    private final String edgeLabel;
    private final String externalVertexLabel;
    private final Direction direction;
    private final String externalVertexIdField;

    public NestedEdgeController(String vertexLabel, String edgeLabel, Direction direction, String externalVertexIdField, String externalVertexLabel) {
        this.vertexLabel = vertexLabel;
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.externalVertexIdField = externalVertexIdField;
    }

    @Override
    public InnerEdge createEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        if(!label.equals(edgeLabel)) return null;
        ElasticStarVertex starVertex = (ElasticStarVertex) (direction.equals(Direction.IN) ? inV : outV);
        if(!starVertex.label().equals(vertexLabel)) return null;
        return new NestedEdge(starVertex, edgeId, edgeLabel, this, outV, inV, properties);
    }

    @Override
    public Set<InnerEdge> parseEdges(ElasticStarVertex vertex, Map<String, Object> keyValues) {
        Object nested = keyValues.get(edgeLabel);
        if(nested == null) return SetUtils.emptySet();
        keyValues.remove(edgeLabel);
        if(nested instanceof Map){
            InnerEdge edge = createEdge(vertex, (Map<String, Object>) nested);
            return Collections.singleton(edge);
        }
        else if(nested instanceof List){
            List<Map<String, Object>> edgesMaps = (List<Map<String, Object>>) nested;
            return edgesMaps.stream().map(edgeMap -> createEdge(vertex, edgeMap)).collect(Collectors.toSet());
        }
        else throw new IllegalArgumentException(nested.toString());
    }

    private InnerEdge createEdge(ElasticStarVertex vertex, Map<String, Object> keyValues) {
        Object externalVertexId = keyValues.get(externalVertexIdField);
        BaseVertex externalVertex = vertex.getGraph().getControllerManager().fromEdge(direction.opposite(), externalVertexId, externalVertexLabel);
        BaseVertex outV = direction.equals(Direction.OUT) ? vertex : externalVertex;
        BaseVertex inV = direction.equals(Direction.IN) ? vertex : externalVertex;
        return createEdge(null, edgeLabel, outV, inV,keyValues);
    }

    @Override
    public FilterBuilder getFilter(List<HasContainer> hasContainers) {
        if(hasContainers == null || hasContainers.size() == 0)
            return FilterBuilders.nestedFilter(edgeLabel, QueryBuilders.matchAllQuery());
        return FilterBuilders.nestedFilter(edgeLabel, ElasticHelper.createFilterBuilder(hasContainers));
    }

    @Override
    public FilterBuilder getFilter(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        if(!direction.equals(this.direction) && !direction.equals(Direction.BOTH)) return null;
        if(!Arrays.asList(edgeLabels).contains(edgeLabel)) return null;

        ArrayList ids = new ArrayList();
        for(Vertex vertex : vertices) {
            if(vertex.label().equals(externalVertexLabel))
                ids.add(vertex.id());
        }

        if(ids.size() == 0) return null;

        List<HasContainer> hasContainers = (List<HasContainer>) predicates.hasContainers.clone();
        hasContainers.add(new HasContainer(externalVertexIdField, P.within(ids.toArray())));
        return getFilter(hasContainers);
    }

    @Override
    public void addEdgeFields(List<InnerEdge> edges, Map<String, Object> map) {
        Map<String, Object>[] edgesMap = new Map[edges.size()];
        for(int i = 0; i< edges.size(); i++){
            InnerEdge innerEdge = edges.get(i);
            Map<String, Object> fields = innerEdge .allFields();
            fields.put(externalVertexIdField, innerEdge.vertices(direction.opposite()).next().id());
            edgesMap[i] = fields;
        }
        map.put(edgeLabel, edgesMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NestedEdgeController that = (NestedEdgeController) o;

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

    @Override

    public Direction getDirection() {
        return direction;
    }
}
