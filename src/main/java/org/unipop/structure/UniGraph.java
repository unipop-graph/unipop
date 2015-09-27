package org.unipop.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;
import org.unipop.controllerprovider.ControllerProvider;
import org.unipop.process.UniGraphStrategy;
import org.unipop.controllerprovider.SimpleControllerProvider;

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
public class UniGraph implements Graph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(UniGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(UniGraphStrategy.instance()));
    }

    //for testSuite
    public static UniGraph open(final Configuration configuration) throws InstantiationException {
        return new UniGraph(configuration);
    }

    private UnipopFeatures features = new UnipopFeatures();
    private final Configuration configuration;
    private ControllerProvider controllerProvider;

    public UniGraph(Configuration configuration) throws InstantiationException {
        try {
            configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());
            this.configuration = configuration;
            String queryHandlerName = configuration.getString("controllerProvider");
            if(queryHandlerName != null) this.controllerProvider = (ControllerProvider)Class.forName(queryHandlerName).newInstance();
            else this.controllerProvider = new SimpleControllerProvider();
            this.getControllerProvider().init(this, configuration);
        } catch(Exception ex) {
            InstantiationException instantiationException = new InstantiationException();
            instantiationException.addSuppressed(ex);
            throw instantiationException;
        }
    }

    public ControllerProvider getControllerProvider() {
        return controllerProvider;
    }

    public void commit() { controllerProvider.commit(); }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, controllerProvider.toString());
    }

    @Override
    public void close() {
        controllerProvider.close();
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
    public Iterator<Vertex> vertices(Object... ids) {
        if(ids.length == 0) return this.traversal().V();

        if (ids.length > 1 && !ids[0].getClass().equals(ids[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if (Vertex.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator(ids);
        else return controllerProvider.getVertexHandler(ids).vertices(ids);
    }

    @Override
    public Iterator<Edge> edges(Object... ids) {
        if(ids.length == 0) return this.traversal().E();

        if (ids.length > 1 && !ids[0].getClass().equals(ids[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if (Edge.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator(ids);
        return controllerProvider.getEdgeHandler(ids).edges(ids);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        return controllerProvider.addVertex(idValue, label, keyValues).addVertex(idValue, label, keyValues);
    }
}
