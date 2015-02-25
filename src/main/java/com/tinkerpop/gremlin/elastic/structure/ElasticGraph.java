package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.elastic.ElasticService;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy.ElasticGraphStepStrategy;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import java.io.IOException;
import java.util.Iterator;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class ElasticGraph implements Graph, Graph.Iterators {
    static {
        try {
            TraversalStrategies.GlobalCache.registerStrategies(ElasticGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(ElasticGraphStepStrategy.instance()));
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    //for testSuite
    public static ElasticGraph open(final Configuration configuration) throws IOException {
        return new ElasticGraph(configuration);
    }

    private final Configuration configuration;
    public final ElasticService elasticService;

    public ElasticGraph(Configuration configuration) throws IOException {
        this.configuration = configuration;
        configuration.setProperty(Graph.GRAPH, ElasticGraph.class.getName());
        elasticService = new ElasticService(this,
                configuration.getString("elasticsearch.cluster.name"),
                configuration.getString("elasticsearch.index.name"),
                configuration.getBoolean("elasticsearch.local"),
                configuration.getBoolean("elasticsearch.refresh"));

    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, elasticService.toString());
    }

    @Override
    public void close() {
        elasticService.close();
    }

    @Override
    public Features features() {
        return new ElasticFeatures();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Exceptions.variablesNotSupported();
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        return elasticService.searchVertices(getIdFilter(vertexIds));
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        return elasticService.searchEdges(getIdFilter(edgeIds));
    }

    private FilterBuilder getIdFilter(Object[] ids) {
        if (ids.length == 0) return null;

        String[] stringIds = new String[ids.length];
        for(int i = 0; i < ids.length; i++)
            stringIds[i] = ids[i].toString();

        return FilterBuilders.idsFilter().addIds(stringIds);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        IndexResponse response = elasticService.addElement(label, idValue, ElasticElement.Type.vertex, keyValues);
        final ElasticVertex vertex = new ElasticVertex(response.getId(), label, this);
        vertex.addPropertiesLocal(keyValues);
        return vertex;
    }
}
