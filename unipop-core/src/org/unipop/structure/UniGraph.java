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
import org.unipop.query.controller.ConfigurationControllerManager;
import org.unipop.query.controller.ControllerManager;
import org.unipop.process.strategyregistrar.StandardStrategyRegistrar;
import org.unipop.process.strategyregistrar.StrategyRegistrar;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.test.UnipopGraphProvider;
import org.unipop.test.UnipopStructureSuite;

import java.util.*;

import static org.unipop.common.util.StreamUtils.asStream;

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
@Graph.OptIn(UnipopGraphProvider.OptIn.UnipopStructureSuite)
@Graph.OptIn(UnipopGraphProvider.OptIn.UnipopProcessSuite)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class UniGraph implements Graph {


    //for testSuite
    public static UniGraph open(final Configuration configuration) throws Exception {
        return new UniGraph(configuration);
    }

    private UniFeatures features = new UniFeatures();
    private Configuration configuration;
    private ControllerManager controllerManager;
    private StrategyRegistrar strategyRegistrar;
    private List<SearchQuery.SearchController> queryControllers;

    public UniGraph(Configuration configuration) throws Exception {
        configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());
        this.configuration = configuration;

        ConfigurationControllerManager configurationControllerManager = new ConfigurationControllerManager(this, configuration);
        StrategyRegistrar strategyRegistrar = determineStartegyRegistrar(configuration);

        init(configurationControllerManager, strategyRegistrar);

        this.queryControllers = controllerManager.getControllers(SearchQuery.SearchController.class);
    }

    public UniGraph(ControllerManager controllerManager, StrategyRegistrar strategyRegistrar) throws Exception {
        init(controllerManager, strategyRegistrar);

    }

    private void init(ControllerManager controllerManager, StrategyRegistrar strategyRegistrar) {
        this.strategyRegistrar = strategyRegistrar;
        this.strategyRegistrar.register();

        this.controllerManager = controllerManager;
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
        return this.query(Vertex.class, ids);
    }

    @Override
    public Iterator<Edge> edges(Object... ids) {
        return this.query(Edge.class, ids);
    }

    private <E extends Element> Iterator<E> query(Class<E> returnType, Object[] ids) {
        ElementHelper.validateMixedElementIds(returnType, ids);
        if (ids.length > 0 && Vertex.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator(ids);
        PredicatesHolder predicatesHolder = new PredicatesHolder(PredicatesHolder.Clause.And);
        if(ids.length > 0) {
            HasContainer idPredicate = new HasContainer(T.id.toString(), P.within(ids));
            predicatesHolder.add(idPredicate);
        }
        SearchQuery<E> uniQuery = new SearchQuery<>(returnType, predicatesHolder, -1, null);
        return queryControllers.stream().<E>flatMap(controller -> asStream(controller.search(uniQuery))).iterator();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {

        Map<String, Object> stringObjectMap = asMap(keyValues);

        return controllerManager.getControllers(AddVertexQuery.AddVertexController.class).stream()
                .map(controller -> controller.addVertex(new AddVertexQuery(stringObjectMap, null)))
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
