package org.elasticgremlin.queryhandler.elasticsearch.edgedoc;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class DocEdgeHandler implements EdgeHandler {
    private ElasticGraph graph;
    private final Client client;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final int scrollSize;
    private final boolean refresh;
    private TimingAccessor timing;

    public DocEdgeHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
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
    public Iterator<Edge> edges() {
        return new QueryIterator<>(FilterBuilders.existsFilter(DocEdge.InId), 0, scrollSize, Integer.MAX_VALUE,
                client, this::createEdge, refresh, timing, indexName);
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
            elements.add(createEdge(response));
        }
        return elements.iterator();
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(DocEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createEdge, refresh, timing, indexName);
    }

    @Override
    public Iterator<Edge> edges(Iterator<Vertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Map<Object, Vertex> idToVertex = new HashMap<>();
        vertices.forEachRemaining(singleVertex -> idToVertex.put(singleVertex.id(), singleVertex));

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        Object[] vertexIds = idToVertex.keySet().toArray();
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        if (direction == Direction.IN)
            boolFilter.must(FilterBuilders.termsFilter(DocEdge.InId, vertexIds));
        else if (direction == Direction.OUT)
            boolFilter.must(FilterBuilders.termsFilter(DocEdge.OutId, vertexIds));
        else if (direction == Direction.BOTH)
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(DocEdge.InId, vertexIds),
                    FilterBuilders.termsFilter(DocEdge.OutId, vertexIds)));

        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow, client, this::createEdge , refresh, timing, indexName);
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        DocEdge elasticEdge = new DocEdge(edgeId, label, properties, outV, inV,graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    private Edge createEdge(SearchHit hit) {
        Map<String, Object> fields = hit.getSource();
        BaseVertex outVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.OutId), fields.get(DocEdge.OutLabel).toString(), null, Direction.OUT);
        BaseVertex inVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.InId), fields.get(DocEdge.InLabel).toString(), null, Direction.IN);
        BaseEdge edge = new DocEdge(hit.getId(), hit.getType(), null, outVertex, inVertex, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }

    private Edge createEdge(GetResponse hit) {
        Map<String, Object> fields = hit.getSource();
        BaseVertex outVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.OutId), fields.get(DocEdge.OutLabel).toString(), null, Direction.OUT);
        BaseVertex inVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.InId), fields.get(DocEdge.InLabel).toString(), null, Direction.IN);
        BaseEdge edge = new DocEdge(hit.getId(), hit.getType(), null, outVertex, inVertex, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }
}
