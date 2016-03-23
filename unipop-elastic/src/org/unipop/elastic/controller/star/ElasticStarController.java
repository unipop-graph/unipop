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
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.unipop.controller.EdgeController;
import org.unipop.controller.InnerEdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class ElasticStarController extends ElasticVertexController implements EdgeController {
    private Set<org.unipop.controller.InnerEdgeController> innerEdgeControllers = new HashSet<>();


    public ElasticStarController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                                 int scrollSize, TimingAccessor timing, org.unipop.controller.InnerEdgeController... innerEdgeControllers) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, timing);
        Collections.addAll(this.innerEdgeControllers, innerEdgeControllers);
    }

    public ElasticStarController() {
    }

    @Override
    public void update(BaseVertex vertex, boolean force) {
        if (vertex.removed) return;
        try {
            if (force) {
                elasticMutations.deleteElement(vertex, getDefaultIndex(), null);
                elasticMutations.addElement(vertex, getDefaultIndex(), null, false);
            } else elasticMutations.updateElement(vertex, getDefaultIndex(), null, false);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        super.init(conf, graph);
        for (Map<String, Object> edge : ((List<Map<String, Object>>) conf.get("edges"))) {
            org.unipop.controller.InnerEdgeController innerEdge = ((org.unipop.controller.InnerEdgeController) Class.forName(edge.get("class").toString()).newInstance());
            innerEdge.init(edge);
            innerEdgeControllers.add(innerEdge);
        }
    }

    @Override
    protected UniVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        return createStarVertex(id, label, keyValues);
    }

    @Override
    protected UniVertex createLazyVertex(Object id, String label, ElasticLazyGetter elasticLazyGetter) {
        UniDelayedStarVertex vertex = new UniDelayedStarVertex(id, label, graph.getControllerManager(), graph, innerEdgeControllers);
        vertex.addTransientProperty(new TransientProperty(vertex, "resource", getResource()));
        return vertex;
    }

    private UniVertex createStarVertex(Object id, String label, Map<String, Object> keyValues) {
        UniStarVertex vertex = new UniStarVertex(id, label, null, graph.getControllerManager(), graph, innerEdgeControllers);
        vertex.addTransientProperty(new TransientProperty(vertex, "resource", getResource()));
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


    private Iterable<UniVertex> getVertexIterableForEdges(Predicates predicates) {
        elasticMutations.refresh();

        OrFilterBuilder orFilter = FilterBuilders.orFilter();
        innerEdgeControllers.forEach(controller -> {
            FilterBuilder filter = (FilterBuilder) controller.getFilter(predicates.hasContainers);
            if (filter != null) orFilter.add(filter);
        });

        QueryIterator<UniVertex> queryIterator = new QueryIterator<>(ElasticHelper.createQuery(new ArrayList<>(), orFilter), scrollSize, predicates.limitHigh, client,
                this::createStarVertex, timing, getDefaultIndex());
        return () -> queryIterator;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        return StreamSupport.stream(getVertexIterableForEdges(predicates).spliterator(), false)
                .map(vertex -> ((UniStarVertex) vertex).getInnerEdges(predicates))
                .flatMap(Collection::stream).iterator();
    }

    @Override
    protected void addProperty(List<BaseVertex> vertices, String key, Object value) {
        super.addProperty(vertices, key, value);
        if (value instanceof List) {
            InnerEdgeController innerEdgeController1 = innerEdgeControllers.stream().filter(innerEdgeController -> innerEdgeController.getEdgeLabel().equals(key)).findFirst().get();
            List<Map<String, Object>> edges = (List<Map<String, Object>>) value;
            vertices.forEach(vertex -> edges.forEach(edge -> innerEdgeController1.parseEdge(((UniStarVertex) vertex), edge)));
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Set<BaseEdge> results = new HashSet<>();
        List<String> labels = Arrays.asList(edgeLabels);
        List<BaseVertex> delayedVertices = Stream.of(vertices)
                .filter(vertex1 -> !((UniVertex) vertex1).hasProperty())
                .map(vertex1 -> ((BaseVertex) vertex1)).collect(Collectors.toList());
        vertexProperties(delayedVertices);

        for (Vertex vertex : vertices) {
            if (vertex instanceof UniStarVertex)
                results.addAll(((UniStarVertex) vertex).getInnerEdges(direction, labels, predicates));
        }

        OrFilterBuilder orFilter = null;
        for (org.unipop.controller.InnerEdgeController controller : innerEdgeControllers) {
            FilterBuilder filter = (FilterBuilder) controller.getFilter(vertices, direction, edgeLabels, predicates);
            if (filter != null) {
                if (orFilter == null) orFilter = FilterBuilders.orFilter();
                orFilter.add(filter);
            }
        }

        if (orFilter != null) {
            elasticMutations.refresh();
//
            QueryIterator<UniVertex> queryIterator = new QueryIterator<>(ElasticHelper.createQuery(new ArrayList<>(), orFilter), scrollSize, predicates.limitHigh, client,
                    this::createVertex, timing, getDefaultIndex());

            queryIterator.forEachRemaining(vertex -> results.addAll(((UniStarVertex) vertex).getInnerEdges(direction.opposite(), labels, predicates)));

        }
        return results.iterator();
    }

//    @Override
//    public long vertexCount(Predicates predicates) {
//        CountResponse count = client.prepareCount(getDefaultIndex()).setQuery(ElasticHelper.createQuery(predicates.hasContainers)).execute().actionGet();
//        return count.getCount();
//    }

    @Override
    public long edgeCount(Predicates predicates) {
        OrFilterBuilder orFilter = null;
        elasticMutations.refresh();

        long[] results = {0};
        for (org.unipop.controller.InnerEdgeController controller : innerEdgeControllers) {
            FilterBuilder filter = (FilterBuilder) controller.getFilter(predicates.hasContainers);
            if (filter != null && predicates.hasContainers.size() > 0) {
                if (orFilter == null) orFilter = FilterBuilders.orFilter();
                orFilter.add(filter);
            }
        }
        QueryBuilder query = QueryBuilders.matchAllQuery();
        if (orFilter != null) {
            elasticMutations.refresh();
            query = ElasticHelper.createQuery(new ArrayList<>(), orFilter);

        }
        final OrFilterBuilder finalOrFilter = orFilter;
        final QueryBuilder finalQuery = query;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(getDefaultIndex()).setQuery(finalQuery);
        innerEdgeControllers.forEach(innerEdgeController -> {
            String edge = innerEdgeController.getEdgeLabel();
            NestedBuilder aggregation = AggregationBuilders.nested(edge).path(edge);
            if (finalOrFilter != null && predicates.hasContainers.size() > 0) {
                aggregation.subAggregation(AggregationBuilders.filter("filter")
                        .filter(ElasticHelper.createFilterBuilder(predicates.hasContainers))
                        .subAggregation(AggregationBuilders.count("count").script("doc")));
            } else {
                aggregation.subAggregation(AggregationBuilders.count("count").script("doc"));
            }
            searchRequestBuilder.addAggregation(aggregation);
        });

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        System.out.println(searchResponse);
        innerEdgeControllers.forEach(innerEdgeController -> {
            String edge = innerEdgeController.getEdgeLabel();
            Map<String, Aggregation> nestedAggregationResult = ((InternalNested) searchResponse.getAggregations().asMap().get(edge)).getAggregations().asMap();
            if (finalOrFilter != null && predicates.hasContainers.size() > 0) {
                results[0] += ((InternalValueCount) ((InternalFilter) nestedAggregationResult.get("filter")).getAggregations().get("count")).getValue();
            } else {
                results[0] += ((InternalValueCount) nestedAggregationResult.get("count")).getValue();

            }
        });

        return results[0];
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
        final long[] results = {0};
        List<String> labels = Arrays.asList(edgeLabels);
        List<BaseVertex> delayedVertices = Stream.of(vertices)
                .filter(vertex1 -> !((UniVertex) vertex1).hasProperty())
                .map(vertex1 -> ((BaseVertex) vertex1)).collect(Collectors.toList());
        vertexProperties(delayedVertices);

        if (!direction.equals(Direction.BOTH))
            for (Vertex vertex : vertices) {
                if (vertex instanceof UniStarVertex)
                    results[0] += (((UniStarVertex) vertex).getInnerEdges(direction, labels, predicates)).size();
            }

        OrFilterBuilder orFilter = null;

        for (org.unipop.controller.InnerEdgeController controller : innerEdgeControllers) {
            FilterBuilder filter = (FilterBuilder) controller.getFilter(vertices, direction, edgeLabels, predicates);
            if (filter != null) {
                if (orFilter == null) orFilter = FilterBuilders.orFilter();
                orFilter.add(filter);
            }
        }
        QueryBuilder query = QueryBuilders.matchAllQuery();
        if (orFilter != null) {
            elasticMutations.refresh();
            query = ElasticHelper.createQuery(new ArrayList<>(), orFilter);

        }
        final OrFilterBuilder finalOrFilter = orFilter;
        final QueryBuilder finalQuery = query;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(getDefaultIndex()).setQuery(finalQuery);
        innerEdgeControllers.forEach(innerEdgeController -> {
            String edge = innerEdgeController.getEdgeLabel();
            if (innerEdgeController.getDirection().equals(direction)) {
                NestedBuilder aggregation = AggregationBuilders.nested(edge).path(edge);
                if (finalOrFilter != null && predicates.hasContainers.size() > 0) {
                    aggregation.subAggregation(AggregationBuilders.filter("filter")
                            .filter(ElasticHelper.createFilterBuilder(predicates.hasContainers))
                            .subAggregation(AggregationBuilders.count("count").script("doc")));
                } else {
                    aggregation.subAggregation(AggregationBuilders.count("count").script("doc"));
                }
                searchRequestBuilder.addAggregation(aggregation);

            }
        });
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        innerEdgeControllers.forEach(innerEdgeController -> {
            if (!innerEdgeController.getDirection().equals(direction)) {
                String edge = innerEdgeController.getEdgeLabel();
                Map<String, Aggregation> nestedAggregationResult = ((InternalNested) searchResponse.getAggregations().asMap().get(edge)).getAggregations().asMap();
                if (finalOrFilter != null && predicates.hasContainers.size() > 0) {
                    results[0] += ((InternalValueCount) ((InternalFilter) nestedAggregationResult.get("filter")).getAggregations().get("count")).getValue();
                } else {
                    results[0] += ((InternalValueCount) nestedAggregationResult.get("count")).getValue();
                }
            }
        });


//        System.out.println(searchResponse);
        return results[0];
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    public void addEdgeMapping(org.unipop.controller.InnerEdgeController mapping) {
        innerEdgeControllers.add(mapping);
    }

}
