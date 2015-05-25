package org.elasticgremlin.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.elasticgremlin.elasticservice.*;
import org.elasticgremlin.process.optimize.ElasticOptimizationStrategy;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;

import java.io.IOException;
import java.util.*;

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX8X_count",
        reason = "too much time. need to implement scroll api.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX3X_count",
        reason = "too much time. need to implement scroll api.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$VertexPropertyFunctionalityTest", method = "shouldSupportNumericIdsIfNumericIdsAreGeneratedFromTheGraph",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$EdgeFunctionalityTest", method = "shouldSupportUserSuppliedIdsOfTypeUuid",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$EdgeFunctionalityTest", method = "shouldSupportUserSuppliedIdsOfTypeAny",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$EdgeFunctionalityTest", method = "shouldSupportUserSuppliedIdsOfTypeNumeric",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$VertexFunctionalityTest", method = "shouldSupportUserSuppliedIdsOfTypeUuid",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$VertexFunctionalityTest", method = "shouldSupportUserSuppliedIdsOfTypeAny",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$VertexFunctionalityTest", method = "shouldSupportUserSuppliedIdsOfTypeNumeric",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP3-695")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphConstructionTest", method = "shouldConstructAnEmptyGraph",
        reason = "need to investigate...")
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class ElasticGraph implements Graph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(ElasticGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(ElasticOptimizationStrategy.instance()));
    }

    //for testSuite
    public static ElasticGraph open(final Configuration configuration) throws IOException {
        return new ElasticGraph(configuration);
    }

    private ElasticFeatures features = new ElasticFeatures();
    private final Configuration configuration;
    public final ElasticService elasticService;

    public ElasticGraph(Configuration configuration) throws IOException {
        this.configuration = configuration;
        configuration.setProperty(Graph.GRAPH, ElasticGraph.class.getName());
        elasticService = new ElasticService(this, configuration);
    }

    /*@Override
    public <C extends TraversalSource> C traversal(TraversalSource.Builder<C> contextBuilder) {
        return (C) contextBuilder.with(new ElasticGraphStepStrategy(elasticService)).create(this);
    }*/


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
        return features;
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
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if(vertexIds.length > 1 && !vertexIds[0].getClass().equals(vertexIds[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if (vertexIds == null || vertexIds.length == 0) return elasticService.searchVertices(new Predicates());
        //return elasticService.getVertices(null,null,vertexIds);
        List<Vertex> vertices = new ArrayList<>();
        for(Object id : vertexIds) {
            if(id instanceof Element)
                id = ((Element)id).id();
            vertices.add(new ElasticVertex(id, null, null, this, true));
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds == null || edgeIds.length == 0) return elasticService.searchEdges(new Predicates(), null);
        return elasticService.getEdges(null, null, edgeIds);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        Vertex v = new ElasticVertex(idValue, label, keyValues, this, false);

        try {
            elasticService.addElement(v, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue);
        }
        return v;
    }
}
