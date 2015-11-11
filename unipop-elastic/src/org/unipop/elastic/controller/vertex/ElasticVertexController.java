package org.unipop.elastic.controller.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ElasticVertexController implements VertexController {
    protected UniGraph graph;
    protected Client client;
    protected ElasticMutations elasticMutations;
    protected final int scrollSize;
    protected TimingAccessor timing;
    private String defaultIndex;
    private Map<Direction, LazyGetter> lazyGetters;

    public ElasticVertexController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                                   int scrollSize, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.defaultIndex = defaultIndex;
        this.scrollSize = scrollSize;
        this.timing = timing;
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        BaseVertex v = createVertex(id, label, properties);
        try {
            elasticMutations.addElement(v, getIndex(properties), null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, Metrics metrics) {
        elasticMutations.refresh(defaultIndex);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh, client,
                this::createVertex, timing, getDefaultIndex());
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return createLazyVertex(vertexId, vertexLabel, getLazyGetter(direction));
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, timing);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    protected ElasticVertex createLazyVertex(Object id, String label,  LazyGetter lazyGetter) {
        return new ElasticVertex(id, label, null, this, graph, lazyGetter, elasticMutations, getDefaultIndex());
    }

    protected ElasticVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        return new ElasticVertex(id, label, keyValues, this, graph, null, elasticMutations, getIndex(keyValues));
    }

    protected String getDefaultIndex() {
        return this.defaultIndex;
    }

    protected String getIndex(Map<String, Object> properties) {
        return getDefaultIndex();
    }
}
