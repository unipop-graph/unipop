package org.unipop.elastic.controller.star;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.star.inneredge.InnerEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class ElasticStarController extends ElasticVertexController implements EdgeController {
    private Set<InnerEdgeController> innerEdgeControllers = new HashSet<>();

    public ElasticStarController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                                 int scrollSize, TimingAccessor timing, InnerEdgeController... innerEdgeControllers) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, timing);
        Collections.addAll(this.innerEdgeControllers, innerEdgeControllers);
    }

    public ElasticStarController(){}

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        super.init(conf, graph);
        for (Map<String, Object> edge : ((List<Map<String, Object>>) conf.get("edges"))) {
            InnerEdgeController innerEdge = ((InnerEdgeController) Class.forName(edge.get("class").toString()).newInstance());
            innerEdge.init(edge);
            innerEdgeControllers.add(innerEdge);
        }
    }

    @Override
    protected ElasticVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        return createStarVertex(id, label, keyValues);
    }

    @Override
    protected ElasticVertex createLazyVertex(Object id, String label, ElasticLazyGetter elasticLazyGetter) {
        return new ElasticStarVertex(id, label, null, graph, elasticLazyGetter, this, elasticMutations, getDefaultIndex(), innerEdgeControllers);
    }

    private ElasticStarVertex createStarVertex(Object id, String label, Map<String, Object> keyValues) {
        ElasticStarVertex vertex = new ElasticStarVertex(id, label, null, graph, null, this, elasticMutations, getDefaultIndex(), innerEdgeControllers);
        if (keyValues != null) {
            innerEdgeControllers.stream().map(controller -> controller.parseEdges(vertex, keyValues)).flatMap(Collection::stream).forEach(vertex::addInnerEdge);
            keyValues.entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        }
        return vertex;
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return innerEdgeControllers.stream()
                .map(mapping -> mapping.addEdge(edgeId, label, outV, inV, properties))
                .filter(i -> i != null)
                .findFirst().get();
    }


    private Iterable<ElasticStarVertex> getVertexIterableForEdges(Predicates predicates) {
        elasticMutations.refresh();

        OrFilterBuilder orFilter = FilterBuilders.orFilter();
        innerEdgeControllers.forEach(controller -> {
            FilterBuilder filter = controller.getFilter(predicates.hasContainers);
            if (filter != null) orFilter.add(filter);
        });

        QueryIterator<ElasticStarVertex> queryIterator = new QueryIterator<>(ElasticHelper.createQuery(new ArrayList<>(), orFilter), scrollSize, predicates.limitHigh, client,
                this::createStarVertex, timing, getDefaultIndex());
        return () -> queryIterator;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        return StreamSupport.stream(getVertexIterableForEdges(predicates).spliterator(), false)
                .map(vertex -> vertex.getInnerEdges(predicates))
                .flatMap(Collection::stream).iterator();
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Set<BaseEdge> results = new HashSet<>();
        List<String> labels = Arrays.asList(edgeLabels);

        for (Vertex vertex : vertices) {
            if (vertex instanceof ElasticStarVertex)
                results.addAll(((ElasticStarVertex) vertex).getInnerEdges(direction, labels, predicates));
        }

        OrFilterBuilder orFilter = null;
        for (InnerEdgeController controller : innerEdgeControllers) {
            FilterBuilder filter = controller.getFilter(vertices, direction, edgeLabels, predicates);
            if (filter != null) {
                if (orFilter == null) orFilter = FilterBuilders.orFilter();
                orFilter.add(filter);
            }
        }

        if (orFilter != null) {
            elasticMutations.refresh();

            QueryIterator<ElasticStarVertex> queryIterator = new QueryIterator<>(ElasticHelper.createQuery(new ArrayList<>(), orFilter), scrollSize, predicates.limitHigh, client,
                    this::createStarVertex, timing, getDefaultIndex());
            queryIterator.forEachRemaining(vertex -> results.addAll(vertex.getInnerEdges(direction.opposite(), labels, predicates)));
        }

        return results.iterator();
    }

    @Override
    public long vertexCount(Predicates predicates) {
        CountResponse count = client.prepareCount(getDefaultIndex()).setQuery(ElasticHelper.createQuery(predicates.hasContainers)).execute().actionGet();
        return count.getCount();
    }

    @Override
    public long edgeCount(Predicates predicates) {
        // TODO: update to count not only inner edges
        return StreamSupport.stream(getVertexIterableForEdges(predicates).spliterator(), false)
                .mapToInt(vertex -> vertex.getInnerEdges(predicates).size()).sum();

    }

    public Vertex[] getVerticesByIds(Vertex[] vertices, Set<Object> ids) {
        ArrayList<Vertex> vertexArrayList = new ArrayList<>();
        for (Vertex vertex : vertices) {
            if (ids.contains(vertex.id()))
                vertexArrayList.add(vertex);
        }
        return vertexArrayList.toArray(new Vertex[vertexArrayList.size()]);
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        final long[] count = {0};
        innerEdgeControllers.forEach(innerEdgeController -> {
            List<Object> idsList = Stream.of(vertices).map(Element::id).collect(Collectors.toList());
            innerEdgeController.getFilter(vertices, direction, edgeLabels, predicates);
            SearchRequestBuilder requestBuilder = client.prepareSearch(getDefaultIndex())
                    .setQuery(ElasticHelper.createQuery(new ArrayList<>(), innerEdgeController.getFilter(vertices, direction, edgeLabels, predicates)));
            HashMap<Object, Integer> idsCount = new HashMap<>();
            idsList.forEach(id -> idsCount.put(id, (idsCount.containsKey(id) ? idsCount.get(id) : 0) + 1));
            ArrayList<NestedBuilder> aggs = new ArrayList<>();
            int aggregationsCounter = 1;
            while (!idsCount.isEmpty()) {
                NestedBuilder agg = AggregationBuilders.nested(Integer.toString(aggregationsCounter))
                        .path(innerEdgeController.getEdgeLabel());
                agg.subAggregation(AggregationBuilders.filter("filtered").filter(innerEdgeController.getFilter(getVerticesByIds(vertices, idsCount.keySet()), direction, edgeLabels, predicates)));
                aggs.add(agg);
                idsCount.keySet().forEach(id -> idsCount.put(id, idsCount.get(id) - 1));
                Object[] idArray = idsCount.keySet().toArray();
                for (int i = 0; i < idArray.length; i++) {
                    if (idsCount.get(idArray[i]) == 0) idsCount.remove(idArray[i]);
                }
                aggregationsCounter++;
            }
            aggs.forEach(requestBuilder::addAggregation);
            SearchResponse response = requestBuilder.execute().actionGet();
            List<Aggregation> agg = response.getAggregations().asList();
            agg.forEach(ag -> count[0] += ((InternalFilter) ((InternalNested) ag).getAggregations().get("filtered")).getDocCount());

        });
        return count[0];
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    public void addEdgeMapping(InnerEdgeController mapping) {
        innerEdgeControllers.add(mapping);
    }

}
