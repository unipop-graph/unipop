package org.unipop.structure;

import groovyjarjarcommonscli.MissingArgumentException;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.process.UniGraphStrategy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method = "shouldNotGetConcurrentModificationException",
        reason = "need to investigate...")
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
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
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
    private ControllerManager controllerManager;

    public UniGraph(Configuration configuration) throws InstantiationException {
        try {
            configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());
            this.configuration = configuration;
            String queryHandlerName = configuration.getString("controllerManager");
            if(queryHandlerName != null) this.controllerManager = (ControllerManager)Class.forName(queryHandlerName).newInstance();
            else throw new MissingArgumentException("No ControllerManager configured.");//this.controllerManager = new SimpleControllerProvider();
            this.getControllerManager().init(this, configuration);
        } catch(Exception ex) {
            InstantiationException instantiationException = new InstantiationException();
            instantiationException.addSuppressed(ex);
            throw instantiationException;
        }
    }

    public ControllerManager getControllerManager() {
        return controllerManager;
    }

    public void commit() { controllerManager.commit(); }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, controllerManager.toString());
    }

    @Override
    public void close() {
        controllerManager.close();
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
        else return transform(controllerManager.vertices(ids));
    }

    @Override
    public Iterator<Edge> edges(Object... ids) {
        if(ids.length == 0) return this.traversal().E();

        if (ids.length > 1 && !ids[0].getClass().equals(ids[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if (Edge.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator(ids);
        return transform(controllerManager.edges(ids));
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {

        Map<String, Object> stringObjectMap = asMap(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        stringObjectMap.remove("id");
        stringObjectMap.remove("label");
        return controllerManager.addVertex(idValue, label, stringObjectMap);
    }

    private <E,S>Iterator<E> transform(Iterator<S> source){
        return new TransformIterator<>(source, input -> (E) input);
    }

    public static Map<String, Object> asMap(Object[] keyValues){
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Map<String, Object> map = new HashMap<>();
        if (keyValues != null) {
            //if(keyValues.length % 2 == 1) throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
            for (int i = 0; i < keyValues.length; i = i + 2) {
                String key = keyValues[i].toString();
                Object value = keyValues[i + 1];
                ElementHelper.validateProperty(key,value);
                map.put(key, value);
            }
        }
        return map;
    }
}
