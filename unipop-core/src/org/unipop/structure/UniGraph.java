package org.unipop.structure;

import org.apache.commons.collections4.iterators.ArrayIterator;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.common.test.UnipopGraphProvider;
import org.unipop.process.strategyregistrar.StandardStrategyProvider;
import org.unipop.process.strategyregistrar.StrategyProvider;
import org.unipop.query.controller.ConfigurationControllerManager;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.unipop.common.util.ConversionUtils.asStream;

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
    private TraversalStrategies strategies;
    private ControllerManager controllerManager;
    private List<SearchQuery.SearchController> queryControllers;

    public UniGraph(Configuration configuration) throws Exception {
        configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());
        this.configuration = configuration;

        ConfigurationControllerManager configurationControllerManager = new ConfigurationControllerManager(this, configuration);
        StrategyProvider strategyProvider = determineStrategyProvider(configuration);

        init(configurationControllerManager, strategyProvider);
    }

    public UniGraph(ControllerManager controllerManager, StrategyProvider strategyProvider) throws Exception {
        init(controllerManager, strategyProvider);
    }

    private void init(ControllerManager controllerManager, StrategyProvider strategyProvider) {
        this.strategies = strategyProvider.get();
        //TraversalStrategies.GlobalCache.registerStrategies(UniGraph.class, strategies);

        this.controllerManager = controllerManager;
        this.queryControllers = controllerManager.getControllers(SearchQuery.SearchController.class);
    }

    private StrategyProvider determineStrategyProvider(Configuration configuration) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        StrategyProvider strategyProvider = (StrategyProvider)configuration.getProperty("strategyProvider");
        if (strategyProvider == null) {
            String strategyRegistrarClass = configuration.getString("strategyRegistrarClass");
            if (StringUtils.isNotBlank(strategyRegistrarClass)) {
                strategyProvider = (StrategyProvider) Class.forName(strategyRegistrarClass).newInstance();
            }
        }

        if (strategyProvider == null) {
            strategyProvider = new StandardStrategyProvider();
        }

        return strategyProvider;
    }

    public ControllerManager getControllerManager() {
        return controllerManager;
    }

    @Override
    public GraphTraversalSource traversal() {
        return new GraphTraversalSource(this, strategies);
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
        if (ids.length > 0 && Vertex.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator<>(ids);
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.empty();
        if(ids.length > 0) {
//            List<Object> collect = Stream.of(ids).map(id -> {
//                if (id instanceof Element)
//                    return ((Element) id).id();
//                return id;
//            }).collect(Collectors.toList());

            HasContainer idPredicate = new HasContainer(T.id.getAccessor(), P.within(ids));
            predicatesHolder = PredicatesHolderFactory.predicate(idPredicate);
        }
        SearchQuery<E> uniQuery = new SearchQuery<>(returnType, predicatesHolder, -1, null);
        return queryControllers.stream().<E>flatMap(controller -> asStream(controller.search(uniQuery))).iterator();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Optional<String> labelValue = ElementHelper.getLabelValue(keyValues);
        if(labelValue.isPresent()) ElementHelper.validateLabel(labelValue.get());
        Map<String, Object> stringObjectMap = ElementHelper.asMap(keyValues);
        return controllerManager.getControllers(AddVertexQuery.AddVertexController.class).stream()
                .map(controller -> controller.addVertex(new AddVertexQuery(stringObjectMap, null)))
                .findFirst().get();
    }
}
