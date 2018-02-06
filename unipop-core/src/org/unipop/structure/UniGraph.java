package org.unipop.structure;

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
import org.unipop.process.strategyregistrar.StandardStrategyProvider;
import org.unipop.process.strategyregistrar.StrategyProvider;
import org.unipop.query.controller.ConfigurationControllerManager;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.property.type.DateType;
import org.unipop.schema.property.type.NumberType;
import org.unipop.schema.property.type.TextType;
import org.unipop.structure.TraversalFilter.DefaultTraversalFilter;
import org.unipop.structure.TraversalFilter.TraversalFilter;
import org.unipop.test.UnipopGraphProvider;
import org.unipop.util.ConversionUtils;
import org.unipop.util.PropertyTypeFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "Takes too long.")
//@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX3X_count",
//        reason = "Takes too long.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Takes too long.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest", method = "g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX",
        reason = "Need to investigate.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest", method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "Need to investigate.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest", method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "Need to investigate.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest", method = "g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X",
        reason = "Missing feature requirement")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldSetIdOnAddVWithIdPropertyKeySpecifiedAndNameSuppliedAsProperty", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldSetIdOnAddVWithIdPropertyKeySpecifiedAndIdSuppliedAsProperty", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithGeneratedCustomId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddVWithSpecifiedId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddEWithSpecifiedId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldGenerateDefaultIdOnAddEWithGeneratedId", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldSetIdOnAddEWithIdPropertyKeySpecifiedAndNameSuppliedAsProperty", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest", method = "shouldSetIdOnAddEWithNamePropertyKeySpecifiedAndNameSuppliedAsProperty", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertex", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertexFromStart", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddEdge", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddEdgeByPath", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertexPropertyAdded", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertexWithPropertyThenPropertyAdded", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertexPropertyChanged", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertexPropertyPropertyChanged", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddEdgePropertyAdded", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerEdgePropertyChanged", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerRemoveVertex", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerRemoveEdge", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerRemoveVertexProperty", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerRemoveEdgeProperty", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAddVertexPropertyPropertyRemoved", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldTriggerAfterCommit", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldResetAfterRollback", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "shouldWriteToMultiplePartitions", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "shouldWriteVerticesToMultiplePartitions", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "shouldAppendPartitionToEdge", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "shouldThrowExceptionOnVInDifferentPartition", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest", method = "shouldThrowExceptionOnEInDifferentPartition", reason = "jdbc fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachPropertyOfEdgeWhenRemoved", reason = "fails should investigate")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachVertexPropertyWhenChanged", reason = "not all features implemented")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachVertexPropertyWhenRemoved", reason = "not all features implemented")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachPropertyOfEdgeWhenChanged", reason = "not all features implemented")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachVertexPropertyWhenNew", reason = "not all features implemented")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachPropertyOfEdgeWhenNew", reason = "not all features implemented")
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
        PropertyTypeFactory.init(Arrays.asList(TextType.class.getCanonicalName(),
                DateType.class.getCanonicalName(),
                NumberType.class.getCanonicalName()));
        List<PropertySchema.PropertySchemaBuilder> thirdPartyPropertySchemas = new ArrayList<>();
        if(configuration.containsKey("propertySchemas")){
            Stream.of(configuration.getStringArray("propertiesSchemas")).map(classString -> {
                try {
                    return ((PropertySchema.PropertySchemaBuilder) Class.forName(classString).newInstance());
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new IllegalArgumentException("class: " + classString + " not found");
                }
            }).forEach(thirdPartyPropertySchemas::add);
        }

        String traversalFilter = configuration.getString("traversalFilter", DefaultTraversalFilter.class.getCanonicalName());
        TraversalFilter filter = Class.forName(traversalFilter).asSubclass(TraversalFilter.class).newInstance();

                String configurationControllerManagerName = configuration.getString("controllerManager", ConfigurationControllerManager.class.getCanonicalName().toString());
        ControllerManager configurationControllerManager = Class.forName(configurationControllerManagerName)
                .asSubclass(ControllerManager.class)
                .getConstructor(UniGraph.class, Configuration.class, List.class, TraversalFilter.class)
                .newInstance(this, configuration,thirdPartyPropertySchemas, filter);

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
        StrategyProvider strategyProvider = (StrategyProvider) configuration.getProperty("strategyProvider");
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
        return StringFactory.graphString(this, "UniGraph"/*, controllerManager.toString()*/);
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

    private <E extends Element, C extends Comparable> Iterator<E> query(Class<E> returnType, Object[] ids) {
        PredicatesHolder idPredicate = createIdPredicate(ids, returnType);

        SearchQuery<E> uniQuery = new SearchQuery<>(returnType, idPredicate, -1, null, null, null, null);
        return queryControllers.stream().<E>flatMap(controller -> ConversionUtils.asStream(controller.search(uniQuery))).iterator();
    }

    public static <E extends Element> PredicatesHolder createIdPredicate(Object[] ids, Class<E> returnType) {
        ElementHelper.validateMixedElementIds(returnType, ids);
        //if (ids.length > 0 && Vertex.class.isAssignableFrom(ids[0].getClass()))  return new ArrayIterator<>(ids);
        if (ids.length > 0) {
            List<Object> collect = Stream.of(ids).map(id -> {
                if (id instanceof Element)
                    return ((Element) id).id();
                return id;
            }).collect(Collectors.toList());

            HasContainer idPredicate = new HasContainer(T.id.getAccessor(), P.within(collect));
            return PredicatesHolderFactory.predicate(idPredicate);
        }

        return PredicatesHolderFactory.empty();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Optional<String> labelValue = ElementHelper.getLabelValue(keyValues);
        labelValue.ifPresent(ElementHelper::validateLabel);
        return controllerManager.getControllers(AddVertexQuery.AddVertexController.class).stream()
                .map(controller -> controller.addVertex(new AddVertexQuery(ConversionUtils.asMap(keyValues), null)))
                .filter(Objects::nonNull)
                .findFirst().get();
    }


}
