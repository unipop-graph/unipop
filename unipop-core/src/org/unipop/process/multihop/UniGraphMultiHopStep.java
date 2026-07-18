package org.unipop.process.multihop;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.process.UniPredicatesStep;
import org.unipop.process.order.Orderable;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.MultiHopQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Collapses two consecutive adjacency hops into one bulk query when a
 * {@link MultiHopQuery.MultiHopController} can answer; otherwise runs hops sequentially.
 * Final hop may return vertices ({@code out}/{@code in}) or edges ({@code outE}/{@code inE}).
 */
public class UniGraphMultiHopStep<E extends Element> extends UniPredicatesStep<Vertex, E>
        implements ReceivesPredicatesHolder<Vertex, E>, Orderable, Profiling {

    private static final Logger logger = LoggerFactory.getLogger(UniGraphMultiHopStep.class);
    private static final String MID_PROP = "__unipop_mid";
    private static final String SRC_ALIAS = "__unipop_src";

    private final List<MultiHopQuery.HopSpec> hops;
    private final boolean returnsVertex;
    private PredicatesHolder finalVertexPredicates = PredicatesHolderFactory.empty();
    private List<Pair<String, Order>> orders;
    private int limit = -1;
    private StepDescriptor stepDescriptor;
    private final List<MultiHopQuery.MultiHopController> multiHopControllers;
    private final List<SearchVertexQuery.SearchVertexController> vertexControllers;
    private final List<DeferredVertexQuery.DeferredVertexController> deferredVertexControllers;

    public UniGraphMultiHopStep(org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin traversal,
                                UniGraph graph,
                                ControllerManager controllerManager,
                                List<MultiHopQuery.HopSpec> hops,
                                PredicatesHolder finalVertexPredicates,
                                boolean returnsVertex) {
        super(traversal, graph);
        this.hops = hops == null ? Collections.emptyList() : new ArrayList<>(hops);
        this.returnsVertex = returnsVertex;
        this.finalVertexPredicates = finalVertexPredicates == null
                ? PredicatesHolderFactory.empty() : finalVertexPredicates;
        this.multiHopControllers = controllerManager.getControllers(MultiHopQuery.MultiHopController.class);
        this.vertexControllers = controllerManager.getControllers(SearchVertexQuery.SearchVertexController.class);
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DeferredVertexController.class);
        this.stepDescriptor = new StepDescriptor(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterator<Traverser.Admin<E>> process(List<Traverser.Admin<Vertex>> traversers) {
        Map<Object, List<Traverser<Vertex>>> idToTraverser = new HashMap<>();
        List<Vertex> starts = new ArrayList<>(traversers.size());
        for (Traverser.Admin<Vertex> t : traversers) {
            Vertex v = t.get();
            idToTraverser.computeIfAbsent(v.id(), k -> new ArrayList<>(1)).add(t);
            starts.add(v);
        }

        MultiHopQuery query = new MultiHopQuery(starts, hops, finalVertexPredicates, orders, limit,
                propertyKeys, returnsVertex, stepDescriptor);
        logger.debug("MultiHop query: {}", query);

        Iterator<Edge> joined = null;
        for (MultiHopQuery.MultiHopController c : multiHopControllers) {
            joined = c.search(query);
            if (joined != null) break;
        }

        Iterator<Traverser.Admin<E>> out;
        if (joined != null) {
            out = ConversionUtils.asStream(joined)
                    .flatMap(edge -> mapCarrier(edge, idToTraverser))
                    .iterator();
        } else {
            out = sequentialFallback(starts, idToTraverser);
        }

        if (returnsVertex) {
            boolean noProps = propertyKeys != null && propertyKeys.isEmpty();
            if (finalVertexPredicates.notEmpty() || !noProps) {
                return hydrateIfNeeded((Iterator) out);
            }
        }
        return out;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Stream<Traverser.Admin<E>> mapCarrier(Edge edge, Map<Object, List<Traverser<Vertex>>> idToTraverser) {
        Object startId = startIdOf(edge);
        List<Traverser<Vertex>> starts = idToTraverser.get(startId);
        if (starts == null) return Stream.empty();
        Vertex mid = midOf(edge);
        // Intermediate mid is always a Vertex; final may be Vertex or Edge — use raw split for mid.
        Step midStep = this;
        if (returnsVertex) {
            Vertex target = edge.inVertex();
            return starts.stream().map(st -> {
                Traverser.Admin t = st.asAdmin();
                if (mid != null) t = t.split(mid, midStep);
                return (Traverser.Admin<E>) t.split(target, this);
            });
        } else {
            return starts.stream().map(st -> {
                Traverser.Admin t = st.asAdmin();
                if (mid != null) t = t.split(mid, midStep);
                return (Traverser.Admin<E>) t.split(edge, this);
            });
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Traverser.Admin<E> pathSplit(Traverser.Admin start, Object mid, Object end) {
        Step midStep = this;
        Traverser.Admin t = start;
        if (mid != null) t = t.split(mid, midStep);
        return (Traverser.Admin<E>) t.split(end, this);
    }

    private static Object startIdOf(Edge edge) {
        if (edge instanceof UniEdge && edge.properties(SRC_ALIAS).hasNext()) {
            return edge.property(SRC_ALIAS).value();
        }
        // Vertex-final multi-hop uses adjacencyJoinDirected: out = start shell
        if (edge instanceof UniEdge && ((UniEdge) edge).isAdjacencyJoinDirected()) {
            return edge.outVertex().id();
        }
        return edge.outVertex() != null ? edge.outVertex().id() : null;
    }

    private static Vertex midOf(Edge edge) {
        if (edge.properties(MID_PROP).hasNext()) {
            Object m = edge.property(MID_PROP).value();
            if (m instanceof Vertex) return (Vertex) m;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Iterator<Traverser.Admin<E>> sequentialFallback(List<Vertex> starts,
                                                            Map<Object, List<Traverser<Vertex>>> idToTraverser) {
        if (hops.size() == 2) {
            return sequentialTwoHop(starts, idToTraverser);
        }
        return Collections.emptyIterator();
    }

    @SuppressWarnings("unchecked")
    private Iterator<Traverser.Admin<E>> sequentialTwoHop(List<Vertex> starts,
                                                          Map<Object, List<Traverser<Vertex>>> idToTraverser) {
        MultiHopQuery.HopSpec hop0 = hops.get(0);
        MultiHopQuery.HopSpec hop1 = hops.get(1);

        List<Edge> edges0 = hopEdges(starts, hop0, false);
        List<Object[]> midRows = new ArrayList<>();
        List<Vertex> mids = new ArrayList<>();
        for (Edge edge : edges0) {
            for (Vertex[] st : endpoints(edge, hop0.getDirection())) {
                Vertex sourceShell = st[0];
                Vertex mid = st[1];
                if (mid == null || !idToTraverser.containsKey(sourceShell.id())) continue;
                midRows.add(new Object[]{sourceShell.id(), mid});
                mids.add(mid);
            }
        }

        Map<Object, List<Object[]>> midsById = new HashMap<>();
        for (Object[] row : midRows) {
            Vertex mid = (Vertex) row[1];
            midsById.computeIfAbsent(mid.id(), k -> new ArrayList<>()).add(row);
        }

        List<Traverser.Admin<E>> results = new ArrayList<>();
        if (returnsVertex) {
            List<Edge> edges1 = hopEdges(mids, hop1, true);
            for (Edge edge : edges1) {
                for (Vertex[] st : endpoints(edge, hop1.getDirection())) {
                    Vertex midShell = st[0];
                    Vertex fin = st[1];
                    if (fin == null) continue;
                    List<Object[]> origins = midsById.get(midShell.id());
                    if (origins == null) continue;
                    for (Object[] row : origins) {
                        Object originId = row[0];
                        Vertex mid = (Vertex) row[1];
                        List<Traverser<Vertex>> startsFor = idToTraverser.get(originId);
                        if (startsFor == null) continue;
                        for (Traverser<Vertex> st0 : startsFor) {
                            results.add(pathSplit(st0.asAdmin(), mid, fin));
                        }
                    }
                }
            }
        } else {
            // Final hop returns edges from mids
            SearchVertexQuery q = new SearchVertexQuery(Edge.class, mids, hop1.getDirection(),
                    hop1.getEdgePredicates(), limit, propertyKeys, orders, stepDescriptor, traversal);
            List<Edge> edges1 = vertexControllers.stream()
                    .flatMap(c -> ConversionUtils.asStream(c.search(q)))
                    .collect(Collectors.toList());
            for (Edge edge : edges1) {
                // source vertex of this edge is the mid
                Vertex midShell = hop1.getDirection() == Direction.IN ? edge.inVertex() : edge.outVertex();
                if (midShell == null) continue;
                List<Object[]> origins = midsById.get(midShell.id());
                if (origins == null) {
                    // both() not used; try other endpoint
                    Vertex other = hop1.getDirection() == Direction.IN ? edge.outVertex() : edge.inVertex();
                    if (other != null) origins = midsById.get(other.id());
                }
                if (origins == null) continue;
                for (Object[] row : origins) {
                    Object originId = row[0];
                    Vertex mid = (Vertex) row[1];
                    List<Traverser<Vertex>> startsFor = idToTraverser.get(originId);
                    if (startsFor == null) continue;
                    for (Traverser<Vertex> st0 : startsFor) {
                        results.add(pathSplit(st0.asAdmin(), mid, edge));
                    }
                }
            }
        }
        return results.iterator();
    }

    private List<Edge> hopEdges(List<Vertex> vertices, MultiHopQuery.HopSpec hop, boolean last) {
        if (!returnsVertex && last) {
            // edge-return final hop
            SearchVertexQuery q = new SearchVertexQuery(Edge.class, vertices, hop.getDirection(),
                    hop.getEdgePredicates(), limit, propertyKeys, orders, stepDescriptor, traversal);
            return vertexControllers.stream()
                    .flatMap(c -> ConversionUtils.asStream(c.search(q)))
                    .collect(Collectors.toList());
        }
        PredicatesHolder targetPreds = last ? finalVertexPredicates : PredicatesHolderFactory.empty();
        List<Pair<String, Order>> hopOrders = last ? orders : null;
        int hopLimit = last ? limit : -1;
        SearchVertexQuery q = new SearchVertexQuery(Edge.class, vertices, hop.getDirection(),
                hop.getEdgePredicates(), -1, last ? propertyKeys : Collections.emptySet(), null,
                targetPreds, hopOrders, hopLimit, true, stepDescriptor, traversal);
        return vertexControllers.stream()
                .flatMap(c -> ConversionUtils.asStream(c.search(q)))
                .collect(Collectors.toList());
    }

    private List<Vertex[]> endpoints(Edge edge, Direction direction) {
        List<Vertex[]> out = new ArrayList<>();
        Direction mapDir = (edge instanceof UniEdge && ((UniEdge) edge).isAdjacencyJoinDirected())
                ? Direction.OUT : direction;
        Iterator<Vertex> ends = edge.vertices(mapDir);
        while (ends.hasNext()) {
            Vertex sourceShell = ends.next();
            Vertex target = (edge instanceof UniEdge && ((UniEdge) edge).isAdjacencyJoinDirected())
                    ? edge.inVertex()
                    : UniVertex.vertexToVertex(sourceShell, edge, direction);
            out.add(new Vertex[]{sourceShell, target});
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private Iterator<Traverser.Admin<E>> hydrateIfNeeded(Iterator<Traverser.Admin<E>> traversers) {
        List<Traverser.Admin<E>> copy = ConversionUtils.asStream(traversers).collect(Collectors.toList());
        List<DeferredVertex> deferred = copy.stream()
                .map(Traverser::get)
                .filter(v -> v instanceof DeferredVertex)
                .map(v -> (DeferredVertex) v)
                .filter(DeferredVertex::isDeferred)
                .collect(Collectors.toList());
        if (!deferred.isEmpty()) {
            Set<String> fetchKeys = (finalVertexPredicates.notEmpty() && propertyKeys != null && propertyKeys.isEmpty())
                    ? null : propertyKeys;
            DeferredVertexQuery q = new DeferredVertexQuery(deferred, finalVertexPredicates, fetchKeys, orders,
                    stepDescriptor, traversal);
            deferredVertexControllers.forEach(c -> c.fetchProperties(q));
        }
        if (!finalVertexPredicates.notEmpty()) return copy.iterator();
        Set<Object> matched = copy.stream().map(Traverser::get)
                .filter(v -> v instanceof DeferredVertex && !((DeferredVertex) v).isDeferred())
                .map(v -> ((DeferredVertex) v).id())
                .collect(Collectors.toSet());
        return copy.stream()
                .filter(t -> {
                    Element e = t.get();
                    if (!(e instanceof DeferredVertex)) return true;
                    return matched.contains(e.id()) || !((DeferredVertex) e).isDeferred();
                })
                .iterator();
    }

    @Override
    public void addPredicate(PredicatesHolder predicatesHolder) {
    }

    @Override
    public PredicatesHolder getPredicates() {
        return PredicatesHolderFactory.empty();
    }

    public void setVertexPredicates(PredicatesHolder vertexPredicates) {
        this.finalVertexPredicates = vertexPredicates == null ? PredicatesHolderFactory.empty() : vertexPredicates;
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

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    public boolean returnsVertex() {
        return returnsVertex;
    }

    @Override
    public String toString() {
        List<Direction> dirs = hops.stream().map(MultiHopQuery.HopSpec::getDirection).collect(Collectors.toList());
        return StringFactory.stepString(this, dirs, returnsVertex ? "vertex" : "edge");
    }
}
