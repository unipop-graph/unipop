package org.unipop.elastic.controller.edge;

import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.QueryIterator;
import org.unipop.elastic.helpers.TimingAccessor;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.unipop.controller.*;
import org.unipop.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class ElasticEdgeController implements org.unipop.controller.EdgeController {
    private UniGraph graph;
    private final Client client;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final int scrollSize;
    private final boolean refresh;
    private TimingAccessor timing;

    public ElasticEdgeController(UniGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                                 int scrollSize, boolean refresh, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.timing = timing;
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        MultiGetRequest request = new MultiGetRequest().refresh(refresh);
        for (Object id : ids) request.add(indexName, null, id.toString());
        MultiGetResponse responses = client.multiGet(request).actionGet();

        ArrayList<Edge> elements = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
            elements.add(createEdge(response.getId(), response.getType(), response.getSource()));
        }
        return elements.iterator();
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createEdge, refresh, timing, indexName);
    }

    @Override
    public Iterator<Edge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        Object[] vertexIds = new Object[vertices.length];
        for(int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i].id();

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        if (direction == Direction.IN)
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds));
        else if (direction == Direction.OUT)
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
        else if (direction == Direction.BOTH)
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds),
                    FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds)));

        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow, client, this::createEdge , refresh, timing, indexName);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        ElasticEdge elasticEdge = new ElasticEdge(edgeId, label, properties, outV, inV,graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    private Edge createEdge(SearchHit hit) {
        return createEdge(hit.id(), hit.getType(), hit.getSource());
    }

    private Edge createEdge(String id, String label, Map<String, Object> fields) {
        BaseEdge edge = new ElasticEdge(id, label, null, fields.get(ElasticEdge.OutId), fields.get(ElasticEdge.OutLabel).toString(),
                fields.get(ElasticEdge.InId), fields.get(ElasticEdge.InLabel).toString(),  graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }
}
