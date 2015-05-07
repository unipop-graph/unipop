package org.apache.tinkerpop.gremlin.elastic.structure;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.strategy.ElasticGraphStepStrategy;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;

import java.io.IOException;
import java.util.Iterator;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class ElasticGraph implements Graph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(ElasticGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(ElasticGraphStepStrategy.instance()));
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
        elasticService = new ElasticService(this, configuration);
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
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
    public Vertex addVertex(final Object... keyValues) {
        return addVertex(false, keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds == null || vertexIds.length == 0) return elasticService.searchVertices(null, null);
        return elasticService.getVertices(null,null,vertexIds);    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds == null || edgeIds.length == 0) return elasticService.searchEdges(null, null, null, null);
        return elasticService.getEdges(null, null, edgeIds);    }

    public Vertex addVertex(Boolean upsert, final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        try {
            Object id = elasticService.addElement(upsert, label, idValue, ElasticService.ElementType.vertex, keyValues);
            return new ElasticVertex(id, label, keyValues, this, false);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue);
        }
    }


    public Edge addEdge(String label, Object outId,String outLabel, Object inId,String inLabel,Object... keyValues){
        return this.addEdge(false, label, outId, outLabel, inId, inLabel, keyValues);
    }

    public Edge addEdge(Boolean upsert, String label, Object outId,String outLabel, Object inId,String inLabel,Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.validateLabel(outLabel);
        ElementHelper.validateLabel(inLabel);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        try {
            Object id = elasticService.addElement(upsert, label, idValue, ElasticService.ElementType.edge, ArrayUtils.addAll(keyValues, ElasticEdge.InId, inId, ElasticEdge.OutId, outId, ElasticEdge.InLabel, inLabel, ElasticEdge.OutLabel, outLabel));
            return new ElasticEdge(id, label,outId, outLabel,inId,inLabel, keyValues, this);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        }
    }

    public org.elasticsearch.action.bulk.BulkResponse commit() {
        return elasticService.commit();
    }
}
