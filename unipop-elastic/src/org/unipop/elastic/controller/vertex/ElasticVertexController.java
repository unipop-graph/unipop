package org.unipop.elastic.controller.vertex;

import org.unipop.elastic.controller.edge.ElasticEdge;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.controller.*;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class ElasticVertexController implements VertexController {

    protected UniGraph graph;
    protected Client client;
    protected ElasticMutations elasticMutations;
    protected final int scrollSize;
    protected final boolean refresh;
    protected TimingAccessor timing;
    private String defaultIndex;
    private Map<Direction, LazyGetter> lazyGetters;
    private LazyGetter defaultLazyGetter;

    public ElasticVertexController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                                   int scrollSize, boolean refresh, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.defaultIndex = defaultIndex;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.timing = timing;
        this.lazyGetters = new HashMap<>();
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] vertexIds) {
        List<BaseVertex> vertices = new ArrayList<>();
        for(Object id : vertexIds){
            ElasticVertex vertex = createVertex(id.toString(), null, null, getLazyGetter());
            vertices.add(vertex);
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, refresh, timing, getDefaultIndex());
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return createVertex(vertexId,vertexLabel, null, getLazyGetter(direction));
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        BaseVertex v = createVertex(id, label, properties, getLazyGetter());

        try {
            elasticMutations.addElement(v, getIndex(properties), null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    private LazyGetter getLazyGetter() {
        if (defaultLazyGetter == null || !defaultLazyGetter.canRegister()) {
            defaultLazyGetter = new LazyGetter(client, timing, refresh);
        }
        return defaultLazyGetter;
    }

    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, timing, refresh);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    protected ElasticVertex createVertex(Object id, String label, Object[] keyValues, LazyGetter lazyGetter) {
        return new ElasticVertex(id, label, keyValues, graph, lazyGetter, elasticMutations, getIndex(keyValues));
    }

    protected BaseVertex createVertex(SearchHit hit) {
        BaseVertex vertex = createVertex(hit.id(), hit.getType(), null, getLazyGetter());
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }

    protected String getDefaultIndex() {
        return this.defaultIndex;
    }

    protected String getIndex(Object[] properties) {
        return getDefaultIndex();
    }

}
