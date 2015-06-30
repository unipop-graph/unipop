package org.elasticgremlin.elasticsearch.edge;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.*;
import org.elasticgremlin.querying.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class ElasticDocEdgeHandler implements EdgeHandler {
    private ElasticGraph graph;
    private final Client client;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final int scrollSize;
    private final boolean refresh;

    public ElasticDocEdgeHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName, int scrollSize, boolean refresh) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
    }

    @Override
    public Iterator<Edge> edges() {
        return new SearchQuery<>(indexName, FilterBuilders.existsFilter(ElasticDocEdge.InId),
                0, scrollSize, Integer.MAX_VALUE, client, this::createEdge, refresh);
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        MultiGetRequest request = new MultiGetRequest().refresh(refresh);
        for(int i = 0; i < ids.length; i++) request.add(indexName, null, ids[i].toString());
        MultiGetResponse responses = client.multiGet(request).actionGet();

        ArrayList<Edge> elements = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
            elements.add(createEdge(response));
        }
        return elements.iterator();
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticDocEdge.InId));
        return new SearchQuery<>(indexName, boolFilter,
                0, scrollSize, Integer.MAX_VALUE, client, this::createEdge, refresh);
    }

    @Override
    public Iterator<Edge> edges(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        if(edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        if (direction == Direction.IN)
            boolFilter.must(FilterBuilders.termsFilter(ElasticDocEdge.InId, vertex.id()));
        else if (direction == Direction.OUT)
            boolFilter.must(FilterBuilders.termsFilter(ElasticDocEdge.OutId, vertex.id()));
        else if (direction == Direction.BOTH)
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(ElasticDocEdge.InId, vertex.id()),
                    FilterBuilders.termsFilter(ElasticDocEdge.OutId, vertex.id())));

        return new SearchQuery<>(indexName, boolFilter,
                0, scrollSize, Integer.MAX_VALUE, client, this::createEdge, refresh);
    }

    @Override
    public Edge addEdge(Object edgeId, String label,Vertex outV, Vertex inV, Object[] properties) {
        ElasticDocEdge elasticEdge = new ElasticDocEdge(edgeId, label, outV.id(), outV.label(), inV.id(),  inV.label(), properties, graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch(DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    private Edge createEdge(SearchHit hit) {
        Map<String, Object> fields = hit.getSource();
        BaseEdge edge = new ElasticDocEdge(hit.id(), hit.type(), fields.get(ElasticDocEdge.OutId),fields.get(ElasticDocEdge.OutLabel).toString(), fields.get(ElasticDocEdge.InId),fields.get(ElasticDocEdge.InLabel).toString(), null, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }

    private Edge createEdge(GetResponse hit) {
        Map<String, Object> fields = hit.getSource();
        BaseEdge edge = new ElasticDocEdge(hit.getId(), hit.getType(), fields.get(ElasticDocEdge.OutId),fields.get(ElasticDocEdge.OutLabel).toString(), fields.get(ElasticDocEdge.InId),fields.get(ElasticDocEdge.InLabel).toString(), null, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }
}
