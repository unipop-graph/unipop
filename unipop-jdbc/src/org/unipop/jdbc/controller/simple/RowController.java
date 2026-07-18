package org.unipop.jdbc.controller.simple;

import com.google.common.collect.Maps;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.common.util.PredicatesTranslator;
import org.javatuples.Pair;
import org.unipop.jdbc.schemas.MultiHopJoinBuilder;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.jdbc.utils.ContextManager;
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.catalog.Hop;
import org.unipop.schema.catalog.PathHop;
import org.unipop.schema.catalog.PathPlan;
import org.unipop.schema.catalog.SchemaCatalog;
import org.unipop.schema.catalog.SchemaCatalogAware;
import org.unipop.schema.catalog.SchemaContributor;
import org.unipop.schema.element.VertexSchema;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.MultiHopQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.traversalfilter.TraversalFilter;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.MetricsRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 6/12/2016
 */
public class RowController implements SimpleController, SchemaContributor, SchemaCatalogAware,
        MultiHopQuery.MultiHopController {
    protected final static Logger logger = LoggerFactory.getLogger(RowController.class);

    private final ContextManager contextManager;
    private final UniGraph graph;

    protected Set<? extends RowVertexSchema> vertexSchemas;
    protected Set<? extends RowEdgeSchema> edgeSchemas;

    protected List<Query> bulk;
    private final PredicatesTranslator<Condition> predicatesTranslator;

    private TraversalFilter traversalFilter;
    private SchemaCatalog schemaCatalog;

    public <E extends Element> RowController(UniGraph graph, ContextManager contextManager, Set<JdbcSchema> schemaSet, PredicatesTranslator<Condition> predicatesTranslator, TraversalFilter traversalFilter) {
        this.graph = graph;
        this.contextManager = contextManager;

        extractRowSchemas(schemaSet);
        this.predicatesTranslator = predicatesTranslator;
        bulk = new ArrayList<>();

        this.traversalFilter = traversalFilter;
    }

    @Override
    public Set<? extends ElementSchema> contributedSchemas() {
        Set<ElementSchema> schemas = new HashSet<>();
        if (vertexSchemas != null) schemas.addAll(vertexSchemas);
        if (edgeSchemas != null) schemas.addAll(edgeSchemas);
        return schemas;
    }

    @Override
    public boolean supportsJoin(ElementSchema schema) {
        // Exact-class: InnerRowEdgeSchema extends RowEdgeSchema but is not joinable.
        return schema != null && schema.getClass() == RowEdgeSchema.class;
    }

    @Override
    public void setSchemaCatalog(SchemaCatalog catalog) {
        this.schemaCatalog = catalog;
    }

    @Override
    public Iterator<Edge> search(MultiHopQuery query) {
        if (!graph.configuration().getBoolean("jdbc.multiHopPushdown", true)) return null;
        if (schemaCatalog == null) return null;
        if (query.getHops() == null || query.getHops().size() != 2) return null;
        for (MultiHopQuery.HopSpec h : query.getHops()) {
            if (h.getDirection() == null || h.getDirection() == Direction.BOTH) return null;
        }
        // Edge-final multi-join only when explicitly enabled and a limit is folded onto the step.
        // Unbounded out().outE() multi-joins are slower than sequential hop joins in practice.
        if (!query.isReturnsVertex()) {
            if (!graph.configuration().getBoolean("jdbc.multiHopEdgeFinal", false)) return null;
            if (query.getFinalLimit() < 0) return null;
        }

        List<Hop> catalogHops = new ArrayList<>();
        for (int i = 0; i < query.getHops().size(); i++) {
            MultiHopQuery.HopSpec hs = query.getHops().get(i);
            Set<String> edgeLabels = SchemaCatalog.extractClosedLabels(hs.getEdgePredicates());
            Set<String> targetLabels = (i == query.getHops().size() - 1)
                    ? SchemaCatalog.extractClosedLabels(query.getFinalTargetPredicates())
                    : Collections.emptySet();
            catalogHops.add(new Hop(hs.getDirection(), edgeLabels, targetLabels));
        }

        Set<String> startLabels = SchemaCatalog.labelsOf(query.getStarts());
        // Low cap: many open-endpoint schemas explode plan count; sequential 2-hop is faster then.
        int planCap = graph.configuration().getInt("jdbc.multiHopMaxPlans", 8);
        List<PathPlan> plans = schemaCatalog.findPaths(startLabels, catalogHops, planCap);
        if (plans.isEmpty()) {
            // Not a hard miss when topology is open — fall back so sequential can still hit data.
            return null;
        }
        // If we hit the cap, planning was truncated → sequential is safer/faster.
        if (plans.size() >= planCap) {
            return null;
        }

        // If any plan uses a non-joinable edge, fall back to sequential single hops.
        for (PathPlan plan : plans) {
            for (PathHop ph : plan.getHops()) {
                if (!schemaCatalog.canJoin(ph.getEdgeSchema())
                        || ph.getEdgeSchema().getClass() != RowEdgeSchema.class) {
                    return null;
                }
            }
        }

        // Edge-final: plans only differ by mid/final vertex types; SQL joins e0⋈e1 and does not
        // use those. Dedup by (e0, e1, dirs) so we run one query per edge-schema pair.
        List<PathPlan> toRun = query.isReturnsVertex() ? plans : dedupeEdgeFinalPlans(plans);
        if (!query.isReturnsVertex() && toRun.size() > planCap) {
            return null;
        }

        Set<Edge> out = new HashSet<>();
        int ran = 0;
        try {
            for (PathPlan plan : toRun) {
                Select sel = MultiHopJoinBuilder.buildTwoHop(query, plan);
                if (sel == null) continue;
                ran++;
                for (Map<String, Object> row : this.getContextManager().fetch(sel)) {
                    Edge edge;
                    if (query.isReturnsVertex()) {
                        JdbcVertexSchema midVs = (JdbcVertexSchema) plan.getHops().get(0).getTargetVertexSchema();
                        JdbcVertexSchema finalVs = (JdbcVertexSchema) plan.getHops().get(1).getTargetVertexSchema();
                        edge = MultiHopJoinBuilder.fromTwoHopRow(row, midVs, finalVs, graph);
                    } else {
                        RowEdgeSchema e1 = (RowEdgeSchema) plan.getHops().get(1).getEdgeSchema();
                        edge = MultiHopJoinBuilder.fromTwoHopEdgeRow(row, e1, graph);
                    }
                    if (edge != null) out.add(edge);
                }
            }
        } catch (RowEdgeSchema.OrderNotPushableException e) {
            return null; // all-or-nothing order+limit
        }
        if (ran == 0) return null; // no expressible plan → sequential fallback
        return out.iterator();
    }

    /** Collapse edge-final plans that share the same two edge schemas + directions. */
    private static List<PathPlan> dedupeEdgeFinalPlans(List<PathPlan> plans) {
        Map<String, PathPlan> unique = new LinkedHashMap<>();
        for (PathPlan plan : plans) {
            if (plan.size() != 2) continue;
            PathHop h0 = plan.getHops().get(0);
            PathHop h1 = plan.getHops().get(1);
            String key = System.identityHashCode(h0.getEdgeSchema()) + "|" + h0.getDirection()
                    + "|" + System.identityHashCode(h1.getEdgeSchema()) + "|" + h1.getDirection();
            unique.putIfAbsent(key, plan);
        }
        return new ArrayList<>(unique.values());
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {

        SelectCollector<JdbcSchema<E>, Select, E> collector = new SelectCollector<>(
                schema -> schema.getSearch(uniQuery,
                        schema.toPredicates(uniQuery.getPredicates())),
                (schema, results) -> schema.parseResults(results, uniQuery)
        );
        Set<String> labels = SchemaCatalog.extractClosedLabels(uniQuery.getPredicates());
        Set<? extends JdbcSchema<E>> allSchemas = this.getSchemas(uniQuery.getReturnType());
        Collection<? extends JdbcSchema<E>> schemas = (schemaCatalog == null || labels.isEmpty())
                ? allSchemas
                : schemaCatalog.filterByLabels(allSchemas, labels);

        Map<JdbcSchema<E>, Select> selects = schemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);

        // Offset push: only safe when exactly one schema survives (single table, total order in SQL).
        // Otherwise pushedOffset stays 0 and UniGraphRangeStep applies the full range in memory.
        if (uniQuery.getOffset() > 0 && selects.size() == 1) {
            uniQuery.setPushedOffset(uniQuery.getOffset());
            JdbcSchema<E> only = selects.keySet().iterator().next();
            selects.put(only, only.getSearch(uniQuery, only.toPredicates(uniQuery.getPredicates())));  // rebuild with OFFSET
        }

        return this.search(uniQuery, selects, collector);
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        SelectCollector<JdbcSchema<Vertex>, Select, Vertex> collector = new SelectCollector<>(
                schema -> schema.getSearch(uniQuery,
                        PredicatesHolderFactory.and(
                                ((VertexSchema) schema).toPredicates(uniQuery.getVertices()),
                                schema.toPredicates(uniQuery.getPredicates()))),
                (schema, results) -> schema.parseResults(results, uniQuery)
        );

        Set<String> labels = SchemaCatalog.labelsOf(uniQuery.getVertices());
        // Also honor has() predicates folded into the deferred query
        labels.addAll(SchemaCatalog.extractClosedLabels(uniQuery.getPredicates()));
        Collection<? extends RowVertexSchema> candidates = pruneByLabels(vertexSchemas, labels);

        Map<JdbcSchema<Vertex>, Select> selects = candidates.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);
        Iterator<Vertex> searchIterator = this.search(uniQuery, selects, collector);

        Map<Object, DeferredVertex> vertexMap =
                uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        searchIterator.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if (deferredVertex != null) {
                deferredVertex.loadProperties(newVertex);
            }
        });
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        if (joinApplicable(uniQuery)) {
            Iterator<Edge> joined = searchJoin(uniQuery);
            if (joined != null) return joined;
        }
        // Fallback: existing id-only edge path (catalog prunes by source labels + edge labels).
        SelectCollector<JdbcSchema<Edge>, Select, Edge> collector = new SelectCollector<>(
                schema -> schema.getSearch(uniQuery,
                        ((JdbcEdgeSchema) schema).toPredicates(uniQuery.getVertices(), uniQuery.getDirection(), uniQuery.getPredicates())),
                (schema, results) -> schema.parseResults(results, uniQuery)
        );

        Collection<? extends RowEdgeSchema> edgeCandidates = edgeSchemas;
        if (schemaCatalog != null) {
            Set<String> srcLabels = SchemaCatalog.labelsOf(uniQuery.getVertices());
            Set<ElementSchema> byEndpoint = schemaCatalog.edgesFrom(srcLabels, uniQuery.getDirection());
            edgeCandidates = edgeSchemas.stream()
                    .filter(byEndpoint::contains)
                    .collect(Collectors.toList());
        }
        edgeCandidates = pruneByLabels(edgeCandidates, SchemaCatalog.extractClosedLabels(uniQuery.getPredicates()));

        Map<JdbcSchema<Edge>, Select> selects = edgeCandidates.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);

        return this.search(uniQuery, selects, collector);
    }

    private boolean joinApplicable(SearchVertexQuery q) {
        // ponytail: kill-switch for rollout, remove after bake-in
        if (!graph.configuration().getBoolean("jdbc.joinPushdown", true)) return false;
        if (!q.isHydrateTarget()) return false;
        boolean propertiesRequested = q.getPropertyKeys() == null || !q.getPropertyKeys().isEmpty();
        boolean hasOrder = q.getTargetOrders() != null && !q.getTargetOrders().isEmpty();
        return q.getTargetPredicates().notEmpty() || hasOrder || propertiesRequested;
    }

    /**
     * Join fan-out over (edge schema x candidate vertex schema x direction). BOTH runs as two
     * directed joins (OUT and IN); each produced edge is flagged adjacencyJoinDirected so the vertex
     * step maps it by its own source only (see UniGraphVertexStep), avoiding the double-count that
     * would otherwise come from edge.vertices(BOTH) fanning into both endpoints. Returns null to fall back.
     * <p>
     * When a {@link SchemaCatalog} is present, vertex candidates are pruned via type topology
     * (closed labels / endpoint links). Open endpoints keep today's same-source full fan-out.
     */
    private Iterator<Edge> searchJoin(SearchVertexQuery uniQuery) {
        List<Direction> dirs = uniQuery.getDirection().equals(Direction.BOTH)
                ? Arrays.asList(Direction.OUT, Direction.IN)
                : Collections.singletonList(uniQuery.getDirection());

        Set<String> targetLabels = SchemaCatalog.extractClosedLabels(uniQuery.getTargetPredicates());

        // Keyed by (edgeSchema, vertexSchema, direction) -- BOTH runs the same (edgeSchema, vs) pair
        // once per direction, so direction must be part of the key or one direction's select clobbers
        // the other's map entry.
        Map<Pair<Pair<RowEdgeSchema, JdbcSchema<Vertex>>, Direction>, Select> selects = new HashMap<>();
        try {
            for (JdbcSchema<Edge> es : edgeSchemas) {
                // Exact-class check: InnerRowEdgeSchema (and any other RowEdgeSchema subclass) extends
                // RowEdgeSchema but represents adjacency embedded in the vertex row, not a real joinable
                // edge table -- an `instanceof` check here would wrongly let it through this join path.
                if (es.getClass() != org.unipop.jdbc.schemas.RowEdgeSchema.class) return null; // inner/other edge schemas -> fall back
                if (!this.traversalFilter.filter(es, uniQuery.getTraversal())) continue;
                if (schemaCatalog != null && !schemaCatalog.canJoin(es)) continue;
                RowEdgeSchema edgeSchema = (RowEdgeSchema) es;
                for (Direction dir : dirs) {
                    for (JdbcVertexSchema vs : joinVertexCandidates(edgeSchema, dir, targetLabels)) {
                        if (!this.traversalFilter.filter(vs, uniQuery.getTraversal())) continue;
                        Select sel = edgeSchema.getJoinSearch(uniQuery, vs, dir);
                        if (sel != null) selects.put(new Pair<>(new Pair<>(edgeSchema, vs), dir), sel);
                    }
                }
            }
        } catch (org.unipop.jdbc.schemas.RowEdgeSchema.OrderNotPushableException e) {
            return null; // order column missing on a surviving schema -> full fallback (spec: order+limit push together or not at all)
        }
        if (selects.isEmpty()) return Collections.emptyIterator();

        // NOTE: dedup is by edge id; join edges carry random ids on purpose (see RowEdgeSchema.fromJoinRow).
        Set<Edge> out = new HashSet<>();
        for (Map.Entry<Pair<Pair<RowEdgeSchema, JdbcSchema<Vertex>>, Direction>, Select> entry : selects.entrySet()) {
            RowEdgeSchema edgeSchema = entry.getKey().getValue0().getValue0();
            JdbcVertexSchema vs = (JdbcVertexSchema) entry.getKey().getValue0().getValue1();
            Direction dir = entry.getKey().getValue1();
            for (Map<String, Object> row : this.getContextManager().fetch(entry.getValue())) {
                Edge edge = edgeSchema.fromJoinRow(row, vs, dir);
                if (edge != null) out.add(edge);
            }
        }
        return out.iterator();
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), null, this.graph);
        try {
            if (this.insert(edgeSchemas, edge)) return edge;
        } catch (DataAccessException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }

        return null;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), null, this.graph);
        try {
            if (this.insert(this.vertexSchemas, vertex)) {
                return vertex;
            }
        } catch (DataAccessException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }

        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends JdbcSchema<E>> schemas = this.getSchemas(uniQuery.getElement().getClass());
        if (uniQuery.getAction() == PropertyQuery.Action.Remove) {
            // A plain update rebuilds the SET clause from the element's remaining properties, so a
            // removed key's column would silently keep its old value. Explicitly null the column
            // instead (covers properties().drop(), property(k,null), and mergeE/mergeV onMatch drops).
            this.removeProperty(schemas, uniQuery.getElement(), uniQuery.getProperty());
        } else {
            this.update(schemas, uniQuery.getElement());
        }
    }

    private <E extends Element> void removeProperty(Set<? extends JdbcSchema<E>> schemas, E element, Property property) {
        if (property == null) return;
        for (JdbcSchema<E> schema : schemas) {
            String column = schema.getFieldByPropertyKey(property.key());
            String idField = schema.getFieldByPropertyKey(T.id.getAccessor());
            if (column == null || idField == null) continue;
            JdbcSchema.Row row = schema.toRow(element);
            if (row == null) continue;

            Map<Field<?>, Object> fieldMap = Maps.newHashMap();
            fieldMap.put(field(column), null);
            Update step = DSL.update(table(schema.getTable()))
                    .set(fieldMap).where(field(idField).eq(row.getId()));

            bulk.add(step);
            if (bulk.size() >= 1000) {
                contextManager.batch(bulk);
                bulk.clear();
            }
        }
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(el -> {
            Set<? extends JdbcSchema<E>> schemas = this.getSchemas(el.getClass());

            for (JdbcSchema<E> schema : schemas) {
                DeleteWhereStep deleteStep = DSL.delete(table(schema.getTable()));

                Condition conditions = this.translateElementsToConditions(schema, Collections.singletonList(el));
                Delete step = deleteStep.where(conditions);

                getContextManager().execute(step);
                logger.debug("removed element. element: {}, schema: {}, command: {}", el, schema, step);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <E extends Element> Set<? extends JdbcSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vertexSchemas.stream().map(v -> (JdbcSchema<E>) v).collect(Collectors.toSet());
        } else {
            return edgeSchemas.stream().map(e -> (JdbcSchema<E>) e).collect(Collectors.toSet());
        }
    }

    private <E extends Element, S extends JdbcSchema<E>> void fillChildren(List<MutableMetrics> children, Map<S, Select> schemas) {
        // Each schema-child metric must be filled from ITS OWN query's timing, keyed by that query's
        // SQL. TimingExecuterListener.timing is a static, never-cleared, unordered map accumulating
        // every query ever run, so indexing into its values() (the old behaviour) assigned children
        // arbitrary/stale timings that didn't add up to the parent (issue #108). children.get(i)
        // corresponds to sqls.get(i) — both iterate the same `schemas` map in order.
        List<Map.Entry<S, Select>> sqls = schemas.entrySet().stream().collect(Collectors.toList());
        for (int i = 0; i < sqls.size() && i < children.size(); i++) {
            // Key by the query rendered inlined under this context's dialect — the exact same form
            // TimingExecuterListener records (ctx.dsl().renderInlined(ctx.query())). A detached
            // Select.toString()/getSQL() renders with the default dialect (limit vs Postgres
            // "fetch next", and formatted vs single-line) and never matches the recorded key.
            org.javatuples.Pair<Long, Integer> timingCount =
                    TimingExecuterListener.timing.get(this.getContextManager().renderInlined(sqls.get(i).getValue()));
            MutableMetrics child = children.get(i);
            // Always set a count/duration (default 0 when no timing was recorded) so the parent
            // controller metric's sum over children can't NPE on an unset child.
            child.setCount(TraversalMetrics.ELEMENT_COUNT_ID, timingCount != null ? timingCount.getValue1() : 0L);
            child.setDuration(timingCount != null ? timingCount.getValue0() : 0L, TimeUnit.NANOSECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    protected <E extends Element, R> Iterator<R> search(UniQuery query, Map<JdbcSchema<E>, Select> selects,
                                                        SelectCollector<JdbcSchema<E>, Select, R> collector) {

        if (bulk.size() != 0) {
            contextManager.batch(bulk);
            bulk.clear();
        }
        MetricsRunner metrics = new MetricsRunner(this, query,
                selects.keySet().stream().map(s -> ((ElementSchema) s)).collect(Collectors.toList()));

        logger.info("mapped schemas for search, schemas: {}", selects);
        if (selects.size() == 0) return EmptyIterator.instance();

        Iterator<JdbcSchema<E>> schemaIterator = selects.keySet().iterator();
        Set<R> collect = selects.values().stream()
                .map(select -> this.getContextManager().fetch(select))
                .flatMap(res -> collector.parse.apply(schemaIterator.next(), res).stream())
                .collect(Collectors.toSet());


        metrics.stop((children) -> fillChildren(children, selects));

        logger.info("results: {}", collect);
        return collect.iterator();
    }

    private <E extends Element> boolean insert(Set<? extends JdbcSchema<E>> schemas, E element) {
        for (JdbcSchema<E> schema : schemas) {
            Query query = schema.getInsertStatement(element);
            if (query == null) continue;
//            int changeSetCount = contextManager.execute(query);
//            if (logger.isDebugEnabled())
//                logger.debug("executed insertion, query: {}", contextManager.render(query));
//            if (changeSetCount == 0) {
//                logger.error("no rows changed on insertion. query: {}, element: {}", contextManager.render(query), element);
//            }
//            else {
            bulk.add(query);
            if (bulk.size() >= 1000) {
                contextManager.batch(bulk);
                bulk.clear();
            }
            return true;
//            }
        }
        return false;
    }

    private <E extends Element> void update(Set<? extends JdbcSchema<E>> schemas, E element) {
        for (JdbcSchema<E> schema : schemas) {
            JdbcSchema.Row row = schema.toRow(element);

            if (row == null) continue;

            Map<Field<?>, Object> fieldMap = Maps.newHashMap();
            row.getFields().entrySet().stream().map(this::mapSet).forEach(en -> fieldMap.put(en.getKey(), en.getValue()));

            Update step = DSL.update(table(schema.getTable()))
                    .set(fieldMap).where(field(schema.getFieldByPropertyKey(T.id.getAccessor())).eq(row.getId()));

//            this.getContextManager().execute(step);
            logger.info("executed update statement with following parameters, step: {}, element: {}, schema: {}", this.getContextManager().render(step), element, schema);
//            contextManager.execute("commit;");
            bulk.add(step);
            if (bulk.size() >= 1000) {
                contextManager.batch(bulk);
                bulk.clear();
            }
        }
    }

    private Map.Entry<Field<?>, Object> mapSet(Map.Entry<String, Object> entry) {
        return new DefaultMapEntry<>(field(entry.getKey()), entry.getValue());
    }

    private <E extends Element> Condition translateElementsToConditions(JdbcSchema<E> schema, List<E> elements) {
        return this.predicatesTranslator.translate(
                new PredicatesHolder(
                        PredicatesHolder.Clause.And,
                        elements.stream()
                                .map(schema::toFields)
                                .map(Map::entrySet)
                                .flatMap(m -> m.stream().map(es -> new HasContainer(es.getKey(), P.within(es.getValue()))))
                                .collect(Collectors.toList()), Collections.emptyList()));

    }

    /**
     * Vertex schemas to probe for a join in {@code dir}. Uses the schema catalog when present;
     * otherwise the full controller vertex set (legacy cartesian fan-out).
     */
    private Collection<JdbcVertexSchema> joinVertexCandidates(RowEdgeSchema edgeSchema, Direction dir, Set<String> targetLabels) {
        if (schemaCatalog == null) {
            return vertexSchemas.stream().map(vs -> (JdbcVertexSchema) vs).collect(Collectors.toList());
        }
        Set<ElementSchema> targets = schemaCatalog.joinTargets(edgeSchema, dir, targetLabels);
        List<JdbcVertexSchema> out = new ArrayList<>();
        for (ElementSchema s : targets) {
            if (s instanceof JdbcVertexSchema) out.add((JdbcVertexSchema) s);
        }
        return out;
    }

    /**
     * Drop schemas whose closed label set cannot match {@code labels}. No catalog → no pruning.
     */
    private <S extends ElementSchema> Collection<? extends S> pruneByLabels(Collection<? extends S> schemas, Set<String> labels) {
        if (schemaCatalog == null || labels == null || labels.isEmpty() || schemas == null) {
            return schemas;
        }
        return schemaCatalog.filterByLabels(schemas, labels);
    }

    private <E extends Element> void extractRowSchemas(Set<JdbcSchema> schemas) {
        logger.debug("extracting row schemas to element schemas, jdbcSchemas: {}", schemas);
        Set<JdbcSchema<E>> JdbcSchemas = collectSchemas(schemas);
        this.vertexSchemas = JdbcSchemas.stream().filter(schema -> schema instanceof RowVertexSchema)
                .map(schema -> ((RowVertexSchema) schema)).collect(Collectors.toSet());
        this.edgeSchemas = JdbcSchemas.stream().filter(schema -> schema instanceof RowEdgeSchema)
                .map(schema -> ((RowEdgeSchema) schema)).collect(Collectors.toSet());
        logger.info("extraced row schemas, vertexSchemas: {}, edgeSchemas: {}", this.vertexSchemas, this.edgeSchemas);
    }

    private <E extends Element> Set<JdbcSchema<E>> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<JdbcSchema<E>> rowSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if (schema instanceof JdbcSchema) {
                rowSchemas.add((JdbcSchema<E>) schema);
                Set<JdbcSchema<E>> childSchemas = collectSchemas(schema.getChildSchemas());
                rowSchemas.addAll(childSchemas);
            }
        });
        return rowSchemas;
    }

    public ContextManager getContextManager() {
        return this.contextManager;
    }

    @Override
    public String toString() {
        return "RowController{" +
                "contextManager=" + contextManager +
                ", graph=" + graph +
                ", vertexSchemas=" + vertexSchemas +
                ", edgeSchemas=" + edgeSchemas +
                ", predicatesTranslator=" + predicatesTranslator +
                '}';
    }

    public class SelectCollector<K, V, R> implements Collector<K, Map<K, V>, Map<K, V>> {

        private final Function<? super K, ? extends V> valueMapper;

        private final BiFunction<? super K, List<Map<String, Object>>, ? extends Collection<R>> parse;

        public SelectCollector(Function<? super K, ? extends V> valueMapper, BiFunction<? super K, List<Map<String, Object>>, Collection<R>> parse) {
            this.valueMapper = valueMapper;
            this.parse = parse;
        }

        @Override
        public Supplier<Map<K, V>> supplier() {
            return HashMap<K, V>::new;
        }

        @Override
        public BiConsumer<Map<K, V>, K> accumulator() {
            return (map, t) -> {
                V value = valueMapper.apply(t);
                if (value != null) map.put(t, value);
            };
        }

        @Override
        public BinaryOperator<Map<K, V>> combiner() {
            return (map1, map2) -> {
                map1.putAll(map2);
                return map1;
            };
        }

        @Override
        public Function finisher() {
            return m -> m;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return EnumSet.of(Collector.Characteristics.IDENTITY_FINISH);
        }
    }
}
