package org.unipop.structure;

import org.apache.commons.configuration2.Configuration;
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
import org.unipop.query.controller.TransactionalController;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.property.type.*;
import org.unipop.structure.traversalfilter.DefaultTraversalFilter;
import org.unipop.structure.traversalfilter.TraversalFilter;
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
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method = "shouldNotGetConcurrentModificationException", reason = "Federated provider: builds an N*N edge set while re-scanning vertices(); each scan forces a refresh and the writes round-trip to the backend, so it does not terminate in reasonable time. Not a correctness gap.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachVertexPropertyWhenNew", reason = "not all features implemented")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest", method = "shouldDetachPropertyOfEdgeWhenNew", reason = "not all features implemented")
// Activated by supportsTransactions() == true. All five write a graph out and read it back into a
// SECOND graph opened at the same time, and this provider maps every test graph onto one shared set of
// tables. The first graph's rows are still uncommitted, and because inserts render as
// "insert .. on conflict do nothing" the second graph's insert of the same generated id must block until
// it learns whether that transaction commits. Isolating them needs per-graph tables, not a transaction fix.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest", method = "shouldReadWriteSelfLoopingEdges", reason = "Two graphs share one set of tables in this provider, so the second blocks on the first's uncommitted rows.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONTest", method = "shouldReadWriteSelfLoopingEdges", reason = "Two graphs share one set of tables in this provider, so the second blocks on the first's uncommitted rows.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONV2Test", method = "shouldReadWriteSelfLoopingEdges", reason = "Two graphs share one set of tables in this provider, so the second blocks on the first's uncommitted rows.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONV3Test", method = "shouldReadWriteSelfLoopingEdges", reason = "Two graphs share one set of tables in this provider, so the second blocks on the first's uncommitted rows.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GryoTest", method = "shouldReadWriteSelfLoopingEdges", reason = "Two graphs share one set of tables in this provider, so the second blocks on the first's uncommitted rows.")
// Same root cause: GraphMigrator copies between two graphs and rolls back both (GraphMigrator:86-87),
// which on this provider means two connections contending for the same rows in the same tables. These are
// parameterized per IO format, and OptOut matches "method[specific]", so each format needs its own entry.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific = "graphml", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific = "graphsonv1", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific = "graphsonv2", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific = "graphsonv3", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateClassicGraph", specific = "gryo-v3", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateModernGraph", specific = "graphml", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateModernGraph", specific = "graphsonv1", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateModernGraph", specific = "graphsonv2", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateModernGraph", specific = "graphsonv3", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "shouldMigrateModernGraph", specific = "gryo-v3", reason = "GraphMigrator drives two graphs that share one set of tables in this provider.")
// Opens several graph instances at once and writes from competing threads. Each instance is its own
// connection pool over the same tables, so they contend on each other's uncommitted rows; the
// per-request transaction boundary gives them nothing to coordinate through. Without the lock-wait
// deadline in ContextManager this would hang outright rather than fail.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "shouldExecuteCompetingThreadsOnMultipleDbInstances", reason = "Multiple graph instances are separate pools over the same tables, so they block on each other's uncommitted rows.")
// Also activated by the flag, but nothing to do with transactions: both set an arbitrary "name" property
// on an EDGE, and the test schema's edge table has no name column and dynamicProperties disabled, so that
// property cannot round-trip at all. The vertex halves of these same tests pass.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "shouldCommitPropertyAutoTransactionByDefault", reason = "Sets an arbitrary edge property; the fixed test schema has no such edge column and dynamicProperties is off.")
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "shouldRollbackPropertyAutoTransactionByDefault", reason = "Sets an arbitrary edge property; the fixed test schema has no such edge column and dynamicProperties is off.")
@Graph.OptIn(UnipopGraphProvider.OptIn.UnipopStructureSuite)
@Graph.OptIn(UnipopGraphProvider.OptIn.UnipopProcessSuite)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class UniGraph implements Graph {

    //for testSuite
    public static UniGraph open(final Configuration configuration) throws Exception {
        return new UniGraph(configuration);
    }

    private UniFeatures features = new UniFeatures(this);
    private Configuration configuration;
    protected TraversalStrategies strategies;
    private ControllerManager controllerManager;
    private List<SearchQuery.SearchController> queryControllers;
    private UniTransaction transaction;
    private boolean transactional;

    public UniGraph(Configuration configuration) throws Exception {
        configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());
        this.configuration = configuration;
        PropertyTypeFactory.init(Arrays.asList(TextType.class.getCanonicalName(),
                DateType.class.getCanonicalName(),
                NumberType.class.getCanonicalName(),
                DoubleType.class.getCanonicalName(),
                FloatType.class.getCanonicalName(),
                IntType.class.getCanonicalName(),
                LongType.class.getCanonicalName()));
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
        // Register the provider strategies under UniGraph in TinkerPop's GlobalCache so that a
        // traversal source built the standard way — traversal().withEmbedded(graph) /
        // new GraphTraversalSource(graph), which is how gremlin-server binds `g` — also gets them.
        // Without this, only our traversal() override (below) carried the strategies, so a
        // withEmbedded source ran with the default strategy set: has() predicates were never pushed
        // to the source query (SELECT ... WHERE true) and were filtered in memory, where a typed
        // value (uuid/timestamptz coerced to java.util.UUID/OffsetDateTime) never matches a String
        // predicate. The provider strategies are stateless (they read the graph from the traversal
        // at apply time), so a class-level registration is safe; traversal() still applies the
        // per-instance/config strategies explicitly.
        TraversalStrategies.GlobalCache.registerStrategies(UniGraph.class, strategies);

        this.controllerManager = controllerManager;
        this.queryControllers = controllerManager.getControllers(SearchQuery.SearchController.class);
        this.transaction = new UniTransaction(this);
        controllersChanged();
    }

    /**
     * Recomputed rather than fixed at startup because ConfigurationControllerManager hot-reloads its
     * providers when a config file changes, which replaces the controllers — and with them the
     * connection pool a transaction would have been using.
     */
    public void controllersChanged() {
        this.transactional = controllerManager != null && computeTransactional();
    }

    /**
     * Atomicity is single-source only. A transaction here is one transaction on one source, so it can
     * only be honoured when exactly one controller performs writes and that controller is
     * transactional. Two mutating sources cannot share a transaction — Postgres cannot span an
     * Elasticsearch index, and two JDBC sources are two connection pools, hence two transactions — so
     * we refuse rather than commit part of a delete and report success.
     */
    private boolean computeTransactional() {
        long mutating = controllerManager.getControllers().stream().filter(UniGraph::mutates).count();
        return mutating == 1 && transactionalControllers().size() == 1;
    }

    List<TransactionalController> transactionalControllers() {
        return controllerManager.getControllers().stream()
                .filter(TransactionalController.class::isInstance)
                .map(TransactionalController.class::cast)
                .collect(Collectors.toList());
    }

    private static boolean mutates(UniQueryController controller) {
        return controller instanceof AddVertexQuery.AddVertexController
                || controller instanceof AddEdgeQuery.AddEdgeController
                || controller instanceof PropertyQuery.PropertyController
                || controller instanceof RemoveQuery.RemoveController;
    }

    boolean isTransactional() {
        return transactional;
    }

    /**
     * Opens the transaction if it is not already open, so the read or write that follows is scoped to it.
     * Required because nothing in gremlin-core opens a provider's transaction — READ_WRITE_BEHAVIOR.AUTO
     * only fires from readWrite() — and the server commits only when tx().isOpen() is true, so without
     * this every write would land on an auto-committing connection and the commit would be skipped.
     *
     * Reads count, not just writes: AUTO is specified as starting a transaction "when a read or a write
     * occurs", and TransactionTest asserts isOpen() straight after a bare read.
     */
    public void readWrite() {
        if (transactional) tx().readWrite();
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
        if (!transactional) throw Exceptions.transactionsNotSupported();
        return transaction;
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
        // ElementHelper.validateMixedElementIds was removed in TinkerPop 3.6 (mixed id/element args are now allowed).
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
        readWrite();
        return controllerManager.getControllers(AddVertexQuery.AddVertexController.class).stream()
                .map(controller -> controller.addVertex(new AddVertexQuery(ConversionUtils.asMap(keyValues), null)))
                .filter(Objects::nonNull)
                .findFirst().get();
    }


}
