package org.elasticgremlin.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.elasticgremlin.process.optimize.ElasticOptimizationStrategy;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.queryhandler.SimpleQueryHandler;

import java.util.*;

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$VertexPropertyFunctionalityTest", method = "shouldSupportNumericIdsIfNumericIdsAreGeneratedFromTheGraph",
        reason = "need to handle ids in VertexProperties")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldHaveExceptionConsistencyWhenFindVertexByIdThatIsNonExistentViaIterator",
        reason = "We don't throw an exception when the vertexdoc doesn't exist, because we support \"lazy vertices\"")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONTest", method = "shouldReadLegacyGraphSON",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest", method = "shouldReadGraphML",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest", method = "shouldReadGraphMLAnAllSupportedDataTypes",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest", method = "shouldReadGraphMLUnorderedElements",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldReadWriteClassic", specific="graphml",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldReadWriteClassicToFileWithHelpers", specific="graphml",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific="graphml",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldReadWriteClassic", specific="gryo",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldReadWriteClassicToFileWithHelpers", specific="gryo",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific="gryo",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphConstructionTest", method = "shouldConstructAnEmptyGraph",
        reason = "need to investigate...")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Takes too long.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "need to investigate...")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "need to investigate...")
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class ElasticGraph implements Graph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(ElasticGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(ElasticOptimizationStrategy.instance()));
    }

    //for testSuite
    public static ElasticGraph open(final Configuration configuration) throws InstantiationException {
        return new ElasticGraph(configuration);
    }

    private ElasticFeatures features = new ElasticFeatures();
    private final Configuration configuration;
    private QueryHandler queryHandler;

    public ElasticGraph(Configuration configuration) throws InstantiationException {
        try {
            configuration.setProperty(Graph.GRAPH, ElasticGraph.class.getName());
            this.configuration = configuration;
            String queryHandlerName = configuration.getString("queryHandler");
            if(queryHandlerName != null) this.queryHandler = (QueryHandler)Class.forName(queryHandlerName).newInstance();
            else this.queryHandler = new SimpleQueryHandler();
            this.getQueryHandler().init(this, configuration);
        } catch(Exception ex) {
            InstantiationException instantiationException = new InstantiationException();
            instantiationException.addSuppressed(ex);
            throw instantiationException;
        }
    }

    public QueryHandler getQueryHandler() {
        return queryHandler;
    }

    public void commit() { queryHandler.commit(); }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, queryHandler.toString());
    }

    @Override
    public void close() {
        queryHandler.close();
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
        if (vertexIds == null || vertexIds.length == 0) return (Iterator<Vertex>) queryHandler.vertices();
        if(vertexIds.length > 1 && !vertexIds[0].getClass().equals(vertexIds[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if(vertexIds[0] instanceof Vertex) {
            ArrayList<Vertex> list = new ArrayList();
            for(int i = 0; i < vertexIds.length; i++) list.add((Vertex) vertexIds[i]);
            return list.iterator();
        }
        return (Iterator<Vertex>) queryHandler.vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds == null || edgeIds.length == 0) return queryHandler.edges();
        if(edgeIds.length > 1 && !edgeIds[0].getClass().equals(edgeIds[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if(edgeIds[0] instanceof Edge) {
            ArrayList<Edge> list = new ArrayList();
            for(int i = 0; i < edgeIds.length; i++) list.add((Edge) edgeIds[i]);
            return list.iterator();
        }
        return queryHandler.edges(edgeIds);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        return queryHandler.addVertex(idValue, label, keyValues);
    }
}
