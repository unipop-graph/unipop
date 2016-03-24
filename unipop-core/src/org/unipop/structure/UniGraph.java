package org.unipop.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;
import org.unipop.controller.Controller;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.structure.manager.ConfigurationControllerManager;
import org.unipop.structure.manager.ControllerManager;
import org.unipop.structure.manager.ControllerProvider;
import org.unipop.controller.Predicates;
import org.unipop.process.strategyregistrar.StandardStrategyRegistrar;
import org.unipop.process.strategyregistrar.StrategyRegistrar;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.unipop.helpers.StreamUtils.asStream;

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONTest", method = "shouldReadLegacyGraphSON",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest", method = "shouldReadGraphML",
        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest", method = "shouldReadGraphMLAnAllSupportedDataTypes",
//        reason = "https://github.com/rmagen/elastic-gremlin/issues/52")
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
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method = "shouldNotGetConcurrentModificationException",
        reason = "java.lang.IllegalStateException: Edge with id ... was removed.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest$VertexPropertyFunctionalityTest", method = "shouldSupportNumericIdsIfNumericIdsAreGeneratedFromTheGraph",
        reason = "need to handle ids in VertexProperties")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldHaveExceptionConsistencyWhenFindVertexByIdThatIsNonExistentViaIterator",
        reason = "elasticsearch - IllegalArgumentException[no terms provided]")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldHaveExceptionConsistencyWhenFindEdgeByIdThatIsNonExistentViaIterator",
        reason = "elasticsearch - IllegalArgumentException[no terms provided]")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "elasticsearch - IllegalArgumentException[no terms provided]")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals", method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "elasticsearch - IllegalArgumentException[no terms provided]")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest", method = "g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile",
        reason = "Duration should be at least the length of the sleep (59ms): 58")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Takes too long.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Takes too long.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "Takes too long.")
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("org.unipop.elastic.schema.misc.CustomTestSuite")
public class UniGraph implements Graph {
    private List<VertexController> vertexControllers;
    private List<EdgeController> edgeControllers;

    //for testSuite
    public static UniGraph open(final Configuration configuration) throws Exception {
        return new UniGraph(configuration);
    }

    private UnipopFeatures features = new UnipopFeatures();
    private Configuration configuration;
    private ControllerManager controllerManager;
    private StrategyRegistrar strategyRegistrar;

    public UniGraph(Configuration configuration) throws Exception {
        configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());
        this.configuration = configuration;

        ConfigurationControllerManager configurationControllerManager = new ConfigurationControllerManager(this, configuration);
        StrategyRegistrar strategyRegistrar = determineStartegyRegistrar(configuration);

        init(configurationControllerManager, strategyRegistrar);
    }

    public UniGraph(ControllerManager controllerManager, StrategyRegistrar strategyRegistrar) throws Exception {
        init(controllerManager, strategyRegistrar);

    }

    private void init(ControllerManager controllerManager, StrategyRegistrar strategyRegistrar) {
        this.strategyRegistrar = strategyRegistrar;
        this.strategyRegistrar.register();

        this.controllerManager = controllerManager;
        this.vertexControllers = controllerManager.getControllers(VertexController.class);
        this.edgeControllers = controllerManager.getControllers(EdgeController.class);
    }

    private StrategyRegistrar determineStartegyRegistrar(Configuration configuration) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        StrategyRegistrar strategyRegistrar = (StrategyRegistrar)configuration.getProperty("strategyRegistrar");
        if (strategyRegistrar == null) {
            String strategyRegistrarClass = configuration.getString("strategyRegistrarClass");
            if (StringUtils.isNotBlank(strategyRegistrarClass)) {
                strategyRegistrar = (StrategyRegistrar) Class.forName(strategyRegistrarClass).newInstance();
            }
        }

        if (strategyRegistrar == null) {
            strategyRegistrar = new StandardStrategyRegistrar();
        }

        return strategyRegistrar;
    }

    public ControllerManager getControllerManager() {
        return controllerManager;
    }

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
        return this.query(vertexControllers, ids);
    }

    @Override
    public Iterator<Edge> edges(Object... ids) {
        return this.query(edgeControllers, ids);
    }

    private <E extends Element, C extends Controller<E>> Iterator<E> query(List<C> controllers, Object[] ids) {
        if (ids.length > 1 && !ids[0].getClass().equals(ids[1].getClass())) throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        if (ids.length > 0 && Vertex.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator(ids);

        Predicates predicates = new Predicates();
        if(ids.length > 0) predicates.hasContainers.add(new HasContainer(T.id.getAccessor(), P.within(ids)));

        Class<? extends Controller> c = VertexController.class;
        return controllers.stream().<E>flatMap(controller -> asStream(controller.query(predicates))).iterator();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        Map<String, Object> stringObjectMap = asMap(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        stringObjectMap.remove("id");
        stringObjectMap.remove("label");
        return controllerManager.getControllers(VertexController.class).stream()
                .map(controller -> controller.addVertex(idValue, label, stringObjectMap))
                .findFirst().get();
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
