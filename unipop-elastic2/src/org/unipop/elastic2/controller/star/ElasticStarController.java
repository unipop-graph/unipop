package org.unipop.elastic2.controller.star;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.OrQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic2.controller.star.inneredge.InnerEdgeController;
import org.unipop.elastic2.controller.vertex.ElasticVertex;
import org.unipop.elastic2.controller.vertex.ElasticVertexController;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.LazyGetter;
import org.unipop.elastic2.helpers.QueryIterator;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.StreamSupport;


public class ElasticStarController extends ElasticVertexController implements EdgeController {

    private Set<InnerEdgeController> innerEdgeControllers = new HashSet<>();

    public ElasticStarController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                                 int scrollSize, TimingAccessor timing, InnerEdgeController... innerEdgeControllers) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, timing);
        Collections.addAll(this.innerEdgeControllers, innerEdgeControllers);
    }

    @Override
    protected ElasticVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        return createStarVertex(id, label, keyValues);
    }

    @Override
    protected ElasticVertex createLazyVertex(Object id, String label, LazyGetter lazyGetter) {
        return new ElasticStarVertex(id, label, null, graph, lazyGetter, this, elasticMutations, getDefaultIndex(), innerEdgeControllers);
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

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        elasticMutations.refresh();

        OrQueryBuilder orQuery = QueryBuilders.orQuery();
        innerEdgeControllers.forEach(controller -> {
            QueryBuilder filter = controller.getQuery(predicates.hasContainers);
            if (filter != null) orQuery.add(filter);
        });

        QueryIterator<ElasticStarVertex> queryIterator = new QueryIterator<>(orQuery, scrollSize, predicates.limitHigh, client, this::createStarVertex, timing, getDefaultIndex());
//        QueryIterator<ElasticStarVertex> queryIterator = new QueryIterator<>(orQuery, scrollSize, 10000, client, this::createStarVertex, timing, getDefaultIndex());

        Iterable<ElasticStarVertex> iterable = () -> queryIterator;
        return StreamSupport.stream(iterable.spliterator(), false)
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

        OrQueryBuilder orQuery = null;
        for (InnerEdgeController controller : innerEdgeControllers) {
            QueryBuilder filter = controller.getQuery(vertices, direction, edgeLabels, predicates);
            if (filter != null) {
                if (orQuery == null) orQuery = QueryBuilders.orQuery();
                orQuery.add(filter);
            }
        }

        if (orQuery != null) {
            elasticMutations.refresh();

            QueryIterator<ElasticStarVertex> queryIterator = new QueryIterator<>(orQuery, scrollSize, predicates.limitHigh, client, this::createStarVertex, timing, getDefaultIndex());
//            QueryIterator<ElasticStarVertex> queryIterator = new QueryIterator<>(orQuery, scrollSize, 10000, client, this::createStarVertex, timing, getDefaultIndex());
            queryIterator.forEachRemaining(vertex -> results.addAll(vertex.getInnerEdges(direction.opposite(), labels, predicates)));
        }

        return results.iterator();
    }

    @Override
    public long edgeCount(Predicates predicates) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    public void addEdgeMapping(InnerEdgeController mapping) {
        innerEdgeControllers.add(mapping);
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }
}
