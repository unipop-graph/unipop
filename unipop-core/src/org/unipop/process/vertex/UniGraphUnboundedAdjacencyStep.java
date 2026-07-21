package org.unipop.process.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.javatuples.Pair;
import org.unipop.process.order.Orderable;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.process.properties.PropertyFetcher;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Start step for an unbounded {@code g.V().out(L)/in(L)/both(L)} adjacency hop: it issues one
 * unbounded-source {@link SearchVertexQuery} (no source-id bound) against the edge store, so a
 * JDBC controller can resolve it as a single {@code edges JOIN target WHERE has()} — skipping the
 * source-vertex scan AND the per-batch id lists — and emits the produced neighbours directly.
 *
 * <p>Used by {@link UnboundedVertexAdjacencyStrategy} only when a search controller advertises
 * {@link org.unipop.query.controller.SupportsUnboundedAdjacency}; other backends keep the
 * {@code g.E().<adjacency>()} rewrite instead. Because the JOIN can still decline (inner edge
 * schemas, no join-capable schema), this step also handles the deferred fallback: plain neighbours
 * are hydrated and filtered by a {@link DeferredVertexQuery}, dropping non-matches while preserving
 * duplicate-id instances (both()).
 */
public class UniGraphUnboundedAdjacencyStep extends AbstractStep<Vertex, Vertex>
        implements ReceivesPredicatesHolder<Vertex, Vertex>, Orderable, Profiling, PropertyFetcher {

    private final Direction direction;
    private PredicatesHolder predicates;                 // edge query (edge-label) predicates
    private PredicatesHolder vertexPredicates = PredicatesHolderFactory.empty(); // target has()
    private List<Pair<String, Order>> orders;
    private int limit = -1;
    private final List<SearchVertexQuery.SearchVertexController> controllers;
    private final List<DeferredVertexQuery.DeferredVertexController> deferredVertexControllers;
    private StepDescriptor stepDescriptor;
    private Iterator<Traverser.Admin<Vertex>> results = EmptyIterator.instance();
    private boolean queried = false;

    public UniGraphUnboundedAdjacencyStep(VertexStep<?> vertexStep, UniGraph graph, ControllerManager controllerManager) {
        super(vertexStep.getTraversal());
        vertexStep.getLabels().forEach(this::addLabel);
        this.direction = vertexStep.getDirection();
        if (vertexStep.getEdgeLabels().length > 0) {
            HasContainer labelsPredicate = new HasContainer(T.label.getAccessor(), P.within(vertexStep.getEdgeLabels()));
            this.predicates = PredicatesHolderFactory.predicate(labelsPredicate);
        } else this.predicates = PredicatesHolderFactory.empty();
        this.controllers = controllerManager.getControllers(SearchVertexQuery.SearchVertexController.class);
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DeferredVertexController.class);
        this.stepDescriptor = new StepDescriptor(this);
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        if (!queried) {
            results = query();
            queried = true;
        }
        if (results.hasNext()) return results.next();
        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser.Admin<Vertex>> query() {
        SearchVertexQuery vertexQuery = new SearchVertexQuery(Edge.class, Collections.emptyList(), direction,
                predicates, /*edgeLimit*/ -1, /*edge propertyKeys*/ null, /*edgeOrders*/ null,
                /*targetPredicates*/ vertexPredicates, /*targetOrders*/ orders, /*targetLimit*/ limit,
                /*hydrateTarget*/ true, /*allSources*/ true, stepDescriptor, traversal);

        List<Vertex> targets = controllers.stream().<Iterator<Edge>>map(controller -> controller.search(vertexQuery))
                .<Edge>flatMap(ConversionUtils::asStream)
                .<Vertex>flatMap(edge -> targetsOf(edge).stream())
                .collect(Collectors.toList());

        // Deferred fallback (edges not produced by the JOIN): the neighbours are unhydrated and
        // unfiltered -- hydrate + filter them by a bounded DeferredVertexQuery, then drop non-matches.
        List<DeferredVertex> deferred = targets.stream()
                .filter(v -> v instanceof DeferredVertex).map(v -> (DeferredVertex) v)
                .filter(DeferredVertex::isDeferred).collect(Collectors.toList());
        if (!deferred.isEmpty()) {
            DeferredVertexQuery query = new DeferredVertexQuery(deferred, vertexPredicates, null, orders, stepDescriptor, traversal);
            deferredVertexControllers.forEach(controller -> controller.fetchProperties(query));
            if (vertexPredicates.notEmpty()) {
                // Keep every instance whose id matched (the filtered fetch un-defers one instance per
                // matched id; both() emits the same id twice, so filter by id -- not by instance).
                Set<Object> matchedIds = deferred.stream().filter(d -> !d.isDeferred())
                        .map(DeferredVertex::id).collect(Collectors.toSet());
                targets = targets.stream()
                        .filter(v -> !(v instanceof DeferredVertex) || matchedIds.contains(((DeferredVertex) v).id()))
                        .collect(Collectors.toList());
            }
        }

        return targets.stream()
                .<Traverser.Admin<Vertex>>map(v -> traversal.getTraverserGenerator().generate(v, (Step) this, 1L))
                .iterator();
    }

    /** The neighbour(s) an edge contributes. JOIN edges carry the hydrated target as inVertex (a
     *  uniform convention, see RowEdgeSchema.fromJoinRow); plain edges map by hop direction. */
    private List<Vertex> targetsOf(Edge edge) {
        if (edge instanceof UniEdge && ((UniEdge) edge).isAdjacencyJoinDirected())
            return Collections.singletonList(edge.inVertex());
        if (direction.equals(Direction.BOTH)) {
            List<Vertex> both = new ArrayList<>(2);
            both.add(edge.outVertex());
            both.add(edge.inVertex());
            return both;
        }
        return Collections.singletonList(direction.equals(Direction.IN) ? edge.outVertex() : edge.inVertex());
    }

    @Override
    public void reset() {
        super.reset();
        this.queried = false;
        this.results = EmptyIterator.instance();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void addPredicate(PredicatesHolder predicatesHolder) {
        this.predicates = PredicatesHolderFactory.and(this.predicates, predicatesHolder);
    }

    @Override
    public PredicatesHolder getPredicates() {
        return predicates;
    }

    public void setVertexPredicates(PredicatesHolder vertexPredicates) {
        this.vertexPredicates = vertexPredicates == null ? PredicatesHolderFactory.empty() : vertexPredicates;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public void setOrders(List<Pair<String, Order>> orders) {
        this.orders = orders;
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        this.stepDescriptor = new StepDescriptor(this, metrics);
    }

    // PropertyFetcher: a vertex-returning adjacency always hydrates all target properties (the JOIN
    // projects the whole target row; the deferred fetch requests all keys), mirroring
    // UniGraphVertexStep's fetch-all-for-vertex behaviour. So key registration is a no-op -- the
    // point of implementing the interface is that UniGraphPropertiesStrategy finds this as the
    // property fetcher for a following by()/values()/has() instead of tripping over none.
    @Override
    public void addPropertyKey(String key) { }

    @Override
    public void fetchAllKeys() { }

    @Override
    public Set<String> getKeys() { return null; }

    @Override
    public String toString() {
        return org.apache.tinkerpop.gremlin.structure.util.StringFactory.stepString(this, this.direction);
    }
}
