package org.unipop.schema.catalog;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.element.ElementSchema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Internal schema catalog backed by a TinkerGraph of types (not instance data).
 * Controllers and optimizers consult this for join fan-out pruning and pushdown eligibility.
 * Not exposed as a user-facing Graph API.
 */
public final class SchemaCatalog {

    public static final String V_SOURCE = "source";
    public static final String V_VTYPE = "vtype";
    public static final String V_ETYPE = "etype";

    public static final String E_BOUND_TO = "boundTo";
    public static final String E_OUT = "out";
    public static final String E_IN = "in";

    public static final String P_SCHEMA = "schema";
    public static final String P_LABEL_OPEN = "labelOpen";
    public static final String P_LABELS = "labels";
    public static final String P_DYNAMIC_PROPS = "dynamicProps";
    public static final String P_PROPERTY_KEYS = "propertyKeys";
    public static final String P_SUPPORTS_JOIN = "supportsJoin";
    public static final String P_SUPPORTS_ORDER = "supportsOrder";
    public static final String P_SUPPORTS_RANGE = "supportsRange";
    public static final String P_OUT_OPEN = "outOpen";
    public static final String P_IN_OPEN = "inOpen";
    public static final String P_SOURCE_ID = "id";
    public static final String P_PROVIDER_CLASS = "providerClass";

    private volatile Graph graph;

    public SchemaCatalog() {
        this.graph = TinkerGraph.open();
    }

    /** Test / advanced: wrap an existing graph (must be empty or already a catalog). */
    SchemaCatalog(Graph graph) {
        this.graph = graph;
    }

    /**
     * Replace catalog contents from controller contributions (load / hot-reload).
     * Builds a fresh graph and atomically swaps it in, so concurrent readers on the
     * request path always see a fully-built graph (never a half-cleared one) without
     * locking. {@code synchronized} only serializes competing rebuilds.
     */
    public synchronized void rebuild(Collection<? extends SchemaContributor> contributors) {
        Graph fresh = TinkerGraph.open();
        SchemaCatalogBuilder.build(fresh, contributors);
        this.graph = fresh;
    }

    /**
     * Vertex schemas whose closed labels intersect {@code closedLabels}, plus every label-open vtype.
     * Empty {@code closedLabels} returns all vertex schemas.
     */
    public Set<ElementSchema> vertexSchemas(Set<String> closedLabels) {
        return typeSchemas(V_VTYPE, closedLabels);
    }

    /**
     * Edge schemas whose closed labels intersect {@code closedLabels}, plus every label-open etype.
     * Empty {@code closedLabels} returns all edge schemas.
     */
    public Set<ElementSchema> edgeSchemas(Set<String> closedLabels) {
        return typeSchemas(V_ETYPE, closedLabels);
    }

    private Set<ElementSchema> typeSchemas(String kind, Set<String> closedLabels) {
        Set<ElementSchema> out = new HashSet<>();
        boolean anyFilter = closedLabels != null && !closedLabels.isEmpty();
        graph.traversal().V().hasLabel(kind).toList().forEach(v -> {
            if (!anyFilter || bool(v, P_LABEL_OPEN) || intersects(labelSet(v), closedLabels)) {
                ElementSchema schema = schemaOf(v);
                if (schema != null) out.add(schema);
            }
        });
        return out;
    }

    /**
     * Candidate target vertex schemas for an adjacency join on {@code edgeSchema} in {@code dir}.
     * <ul>
     *   <li>Open endpoint (or missing topology link) → all same-source vertex schemas
     *       (optionally filtered by closed {@code targetLabels}).</li>
     *   <li>Closed linked endpoints → only catalog-linked vtypes, then target-label filter.</li>
     * </ul>
     * Always includes label-open same-source vtypes when a target-label filter is applied.
     */
    public Set<ElementSchema> joinTargets(ElementSchema edgeSchema, Direction dir, Set<String> targetLabels) {
        Vertex etype = findTypeVertex(V_ETYPE, edgeSchema);
        if (etype == null) {
            // Catalog miss: safe fallback — no pruning signal
            return vertexSchemas(targetLabels);
        }

        boolean out = dir == Direction.OUT;
        // Target endpoint is the opposite of the source endpoint of the hop.
        // OUT hop: source=out, target=in. IN hop: source=in, target=out.
        String targetEdgeLabel = out ? E_IN : E_OUT;
        boolean targetOpen = out ? bool(etype, P_IN_OPEN) : bool(etype, P_OUT_OPEN);

        Set<ElementSchema> candidates = new HashSet<>();
        if (targetOpen) {
            candidates.addAll(sameSourceVertexSchemas(etype));
        } else {
            Iterator<Edge> links = etype.edges(Direction.OUT, targetEdgeLabel);
            boolean anyLink = false;
            while (links.hasNext()) {
                anyLink = true;
                Vertex vtype = links.next().inVertex();
                ElementSchema schema = schemaOf(vtype);
                if (schema != null) candidates.add(schema);
            }
            if (!anyLink) {
                // Closed flags but the builder linked nothing → safe fallback to all same-source.
                candidates.addAll(sameSourceVertexSchemas(etype));
            } else {
                // Open-label vtypes are never linked (they match any label) yet can still hold the
                // endpoint's labels — include same-source open-label vtypes so they aren't pruned.
                candidates.addAll(sameSourceOpenLabelVertexSchemas(etype));
            }
        }

        if (targetLabels == null || targetLabels.isEmpty()) {
            return candidates;
        }
        return candidates.stream()
                .filter(s -> {
                    Vertex v = findTypeVertex(V_VTYPE, s);
                    if (v == null) return true;
                    return bool(v, P_LABEL_OPEN) || intersects(labelSet(v), targetLabels);
                })
                .collect(Collectors.toSet());
    }

    public boolean sameSource(ElementSchema a, ElementSchema b) {
        Vertex va = findTypeVertexAny(a);
        Vertex vb = findTypeVertexAny(b);
        if (va == null || vb == null) return false;
        Vertex sa = sourceOf(va);
        Vertex sb = sourceOf(vb);
        return sa != null && sa.equals(sb);
    }

    public boolean canJoin(ElementSchema edgeSchema) {
        Vertex etype = findTypeVertex(V_ETYPE, edgeSchema);
        return etype != null && bool(etype, P_SUPPORTS_JOIN);
    }

    /**
     * True if the type declares the key, has dynamic properties, or is unknown to the catalog.
     */
    public boolean hasProperty(ElementSchema type, String key) {
        Vertex v = findTypeVertexAny(type);
        if (v == null) return true;
        if (bool(v, P_DYNAMIC_PROPS)) return true;
        return propertyKeys(v).contains(key);
    }

    /**
     * True when every order key is pushable on this type (declared property or dynamic).
     * Unknown types return true (do not block pushdown).
     */
    public boolean canPushOrder(ElementSchema type, Collection<String> orderKeys) {
        if (orderKeys == null || orderKeys.isEmpty()) return true;
        for (String key : orderKeys) {
            if (key == null) continue;
            if (!hasProperty(type, key)) return false;
        }
        return true;
    }

    /**
     * True when at least one catalog type (vertex or edge) can push all order keys.
     * Used by {@link org.unipop.process.order.UniGraphOrderStrategy} to skip folding when
     * no backend schema can honor the order columns.
     */
    public boolean canPushOrderAnywhere(Collection<String> orderKeys) {
        if (orderKeys == null || orderKeys.isEmpty()) return true;
        for (ElementSchema s : vertexSchemas(Collections.emptySet())) {
            if (canPushOrder(s, orderKeys)) return true;
        }
        for (ElementSchema s : edgeSchemas(Collections.emptySet())) {
            if (canPushOrder(s, orderKeys)) return true;
        }
        // Empty catalog → don't block
        return vertexSchemas(Collections.emptySet()).isEmpty()
                && edgeSchemas(Collections.emptySet()).isEmpty();
    }

    /**
     * Keep schemas that can match the closed label set. Open-label and catalog-unknown schemas
     * always survive. Empty {@code closedLabels} returns the input unchanged (as a new set).
     */
    public <S extends ElementSchema> Set<S> filterByLabels(Collection<? extends S> schemas, Set<String> closedLabels) {
        if (schemas == null || schemas.isEmpty()) return Collections.emptySet();
        if (closedLabels == null || closedLabels.isEmpty()) return new LinkedHashSet<>(schemas);
        Set<S> out = new LinkedHashSet<>();
        for (S schema : schemas) {
            if (matchesLabels(schema, closedLabels)) out.add(schema);
        }
        return out;
    }

    /**
     * Whether {@code schema} can produce elements with any of the closed labels.
     * Unknown / open-label types return true.
     */
    public boolean matchesLabels(ElementSchema schema, Set<String> closedLabels) {
        if (closedLabels == null || closedLabels.isEmpty()) return true;
        Vertex v = findTypeVertexAny(schema);
        if (v == null) return true;
        return bool(v, P_LABEL_OPEN) || intersects(labelSet(v), closedLabels);
    }

    /**
     * Closed ~label values from a predicates holder (and nested children). Empty = unconstrained.
     */
    public static Set<String> extractClosedLabels(PredicatesHolder predicates) {
        Set<String> labels = new HashSet<>();
        if (predicates == null || predicates.isAborted()) return labels;
        for (HasContainer has : predicates.getPredicates()) {
            if (has == null || !Objects.equals(T.label.getAccessor(), has.getKey())) continue;
            Object v = has.getValue();
            if (v instanceof Collection) {
                for (Object o : (Collection<?>) v) {
                    if (o != null) labels.add(o.toString());
                }
            } else if (v != null) {
                labels.add(v.toString());
            }
        }
        if (predicates.getChildren() != null) {
            for (PredicatesHolder child : predicates.getChildren()) {
                labels.addAll(extractClosedLabels(child));
            }
        }
        return labels;
    }

    /**
     * Labels of concrete graph elements (e.g. deferred vertices) for fetch pruning.
     */
    public static Set<String> labelsOf(Collection<? extends Element> elements) {
        Set<String> labels = new HashSet<>();
        if (elements == null) return labels;
        for (Element e : elements) {
            if (e != null && e.label() != null) labels.add(e.label());
        }
        return labels;
    }

    /**
     * Edge types that leave vertices whose labels intersect {@code fromLabels} (or all edges if
     * fromLabels is empty / open endpoints apply).
     */
    public Set<ElementSchema> edgesFrom(Set<String> fromLabels, Direction dir) {
        Set<ElementSchema> out = new LinkedHashSet<>();
        boolean constrain = fromLabels != null && !fromLabels.isEmpty();
        for (Vertex etype : graph.traversal().V().hasLabel(V_ETYPE).toList()) {
            ElementSchema edgeSchema = schemaOf(etype);
            if (edgeSchema == null) continue;
            if (!constrain) {
                out.add(edgeSchema);
                continue;
            }
            // Source endpoint for the hop direction
            boolean sourceOpen = dir == Direction.IN ? bool(etype, P_IN_OPEN) : bool(etype, P_OUT_OPEN);
            if (dir == Direction.BOTH) {
                sourceOpen = bool(etype, P_OUT_OPEN) || bool(etype, P_IN_OPEN);
            }
            if (sourceOpen) {
                out.add(edgeSchema);
                continue;
            }
            String linkLabel = dir == Direction.IN ? E_IN : E_OUT;
            if (dir == Direction.BOTH) {
                if (endpointIntersects(etype, E_OUT, fromLabels) || endpointIntersects(etype, E_IN, fromLabels)) {
                    out.add(edgeSchema);
                }
            } else if (endpointIntersects(etype, linkLabel, fromLabels)) {
                out.add(edgeSchema);
            }
        }
        return out;
    }

    /**
     * Vertex types reachable in at most {@code maxHops} hops from {@code fromLabels}
     * (undirected catalog edges: out/in treated as bidirectional reachability).
     * Includes start types when their labels intersect {@code fromLabels}.
     * Open-label vtypes are always included in the frontier expansion.
     */
    public Set<ElementSchema> reachableVertexTypes(Set<String> fromLabels, int maxHops) {
        if (maxHops < 0) return Collections.emptySet();
        Set<ElementSchema> start = vertexSchemas(fromLabels == null ? Collections.emptySet() : fromLabels);
        if (maxHops == 0) return start;

        Set<ElementSchema> reached = new LinkedHashSet<>(start);
        Queue<ElementSchema> frontier = new ArrayDeque<>(start);
        for (int hop = 0; hop < maxHops && !frontier.isEmpty(); hop++) {
            int size = frontier.size();
            for (int i = 0; i < size; i++) {
                ElementSchema v = frontier.poll();
                Vertex vtype = findTypeVertex(V_VTYPE, v);
                if (vtype == null) continue;
                Set<String> labels = bool(vtype, P_LABEL_OPEN) ? Collections.emptySet() : labelSet(vtype);
                // Neighbors via OUT hop (leave v on out endpoint) and IN hop (leave v on in endpoint)
                for (ElementSchema edge : edgesFrom(labels.isEmpty() ? null : labels, Direction.BOTH)) {
                    for (Direction d : new Direction[]{Direction.OUT, Direction.IN}) {
                        for (ElementSchema nbr : joinTargets(edge, d, Collections.emptySet())) {
                            if (reached.add(nbr)) frontier.add(nbr);
                        }
                    }
                }
            }
        }
        return reached;
    }

    /**
     * True if any path of length ≤ {@code maxHops} exists from {@code fromLabels} to {@code toLabels}
     * in the type topology.
     */
    public boolean pathExists(Set<String> fromLabels, Set<String> toLabels, int maxHops) {
        Set<ElementSchema> reachable = reachableVertexTypes(fromLabels, maxHops);
        if (toLabels == null || toLabels.isEmpty()) return !reachable.isEmpty();
        for (ElementSchema s : reachable) {
            if (matchesLabels(s, toLabels)) return true;
        }
        // Open-label targets: any reachability counts
        for (ElementSchema s : reachable) {
            Vertex v = findTypeVertex(V_VTYPE, s);
            if (v != null && bool(v, P_LABEL_OPEN)) return true;
        }
        return false;
    }

    /**
     * Enumerate type-level paths for a hop chain starting from vertices matching {@code startLabels}.
     * Empty start labels → all vertex types. Caps at {@code maxPlans} results (0 or negative → 32).
     * Returns empty when the closed topology admits no path; open endpoints expand candidacy.
     */
    public List<PathPlan> findPaths(Set<String> startLabels, List<Hop> hops, int maxPlans) {
        int cap = maxPlans <= 0 ? 32 : maxPlans;
        if (hops == null || hops.isEmpty()) return Collections.emptyList();

        Set<ElementSchema> starts = vertexSchemas(startLabels == null ? Collections.emptySet() : startLabels);
        // Frontier: current path plan so far + current vertex type at end of path
        List<PathPlan> complete = new ArrayList<>();
        List<PathPlan> frontierPlans = new ArrayList<>();
        List<ElementSchema> frontierVerts = new ArrayList<>();
        for (ElementSchema start : starts) {
            frontierPlans.add(new PathPlan(Collections.emptyList()));
            frontierVerts.add(start);
        }

        for (int hi = 0; hi < hops.size(); hi++) {
            Hop hop = hops.get(hi);
            if (hop.getDirection() == Direction.BOTH) {
                // Expand BOTH as OUT ∪ IN for planning
                List<PathPlan> nextPlans = new ArrayList<>();
                List<ElementSchema> nextVerts = new ArrayList<>();
                expandHop(frontierPlans, frontierVerts, Hop.of(Direction.OUT, hop.getEdgeLabels(), hop.getTargetLabels()),
                        nextPlans, nextVerts, cap);
                if (nextPlans.size() < cap) {
                    expandHop(frontierPlans, frontierVerts, Hop.of(Direction.IN, hop.getEdgeLabels(), hop.getTargetLabels()),
                            nextPlans, nextVerts, cap);
                }
                frontierPlans = nextPlans;
                frontierVerts = nextVerts;
            } else {
                List<PathPlan> nextPlans = new ArrayList<>();
                List<ElementSchema> nextVerts = new ArrayList<>();
                expandHop(frontierPlans, frontierVerts, hop, nextPlans, nextVerts, cap);
                frontierPlans = nextPlans;
                frontierVerts = nextVerts;
            }
            if (frontierPlans.isEmpty()) return Collections.emptyList();
            if (hi == hops.size() - 1) {
                complete.addAll(frontierPlans);
            }
        }
        return complete.size() > cap ? complete.subList(0, cap) : complete;
    }

    private void expandHop(List<PathPlan> frontierPlans, List<ElementSchema> frontierVerts, Hop hop,
                           List<PathPlan> nextPlans, List<ElementSchema> nextVerts, int cap) {
        Direction dir = hop.getDirection();
        for (int i = 0; i < frontierPlans.size() && nextPlans.size() < cap; i++) {
            PathPlan plan = frontierPlans.get(i);
            ElementSchema from = frontierVerts.get(i);
            Vertex vtype = findTypeVertex(V_VTYPE, from);
            Set<String> fromLabels = (vtype != null && !bool(vtype, P_LABEL_OPEN))
                    ? labelSet(vtype) : Collections.emptySet();
            Set<ElementSchema> edges = edgesFrom(fromLabels.isEmpty() ? null : fromLabels, dir);
            for (ElementSchema edge : edges) {
                if (!hop.getEdgeLabels().isEmpty() && !matchesLabels(edge, hop.getEdgeLabels())) continue;
                // Same-source only when both endpoints are catalogued
                if (findTypeVertexAny(from) != null && findTypeVertexAny(edge) != null && !sameSource(from, edge)) {
                    continue;
                }
                for (ElementSchema target : joinTargets(edge, dir, hop.getTargetLabels())) {
                    if (findTypeVertexAny(edge) != null && findTypeVertexAny(target) != null && !sameSource(edge, target)) {
                        continue;
                    }
                    if (nextPlans.size() >= cap) return;
                    PathPlan extended = plan.append(new PathHop(edge, dir, target));
                    nextPlans.add(extended);
                    nextVerts.add(target);
                }
            }
        }
    }

    /** Package-visible for tests. */
    Graph graph() {
        return graph;
    }

    // ---- helpers ----

    private boolean endpointIntersects(Vertex etype, String catalogEdgeLabel, Set<String> labels) {
        Iterator<Edge> links = etype.edges(Direction.OUT, catalogEdgeLabel);
        boolean any = false;
        while (links.hasNext()) {
            any = true;
            Vertex vtype = links.next().inVertex();
            if (bool(vtype, P_LABEL_OPEN) || intersects(labelSet(vtype), labels)) return true;
        }
        // Closed endpoint with no links → does not match
        return !any && bool(etype, catalogEdgeLabel.equals(E_OUT) ? P_OUT_OPEN : P_IN_OPEN);
    }

    private Set<ElementSchema> sameSourceVertexSchemas(Vertex etype) {
        Vertex source = sourceOf(etype);
        Set<ElementSchema> out = new HashSet<>();
        if (source == null) {
            return vertexSchemas(Collections.emptySet());
        }
        source.edges(Direction.IN, E_BOUND_TO).forEachRemaining(e -> {
            Vertex type = e.outVertex();
            if (V_VTYPE.equals(type.label())) {
                ElementSchema schema = schemaOf(type);
                if (schema != null) out.add(schema);
            }
        });
        return out;
    }

    /** Same-source vtypes whose label domain is open (accept any label). */
    private Set<ElementSchema> sameSourceOpenLabelVertexSchemas(Vertex etype) {
        Vertex source = sourceOf(etype);
        Set<ElementSchema> out = new HashSet<>();
        if (source == null) return out;
        source.edges(Direction.IN, E_BOUND_TO).forEachRemaining(e -> {
            Vertex type = e.outVertex();
            if (V_VTYPE.equals(type.label()) && bool(type, P_LABEL_OPEN)) {
                ElementSchema schema = schemaOf(type);
                if (schema != null) out.add(schema);
            }
        });
        return out;
    }

    private Vertex sourceOf(Vertex typeVertex) {
        Iterator<Edge> it = typeVertex.edges(Direction.OUT, E_BOUND_TO);
        return it.hasNext() ? it.next().inVertex() : null;
    }

    // ponytail: O(types) scan + a fresh traversal per call, fine while the catalog is type-sized.
    // Memoize schema->vertex in a per-rebuild map if a large-schema profile ever shows it hurts.
    private Vertex findTypeVertex(String kind, ElementSchema schema) {
        if (schema == null) return null;
        for (Vertex v : graph.traversal().V().hasLabel(kind).toList()) {
            if (Objects.equals(schemaOf(v), schema)) return v;
        }
        return null;
    }

    private Vertex findTypeVertexAny(ElementSchema schema) {
        Vertex v = findTypeVertex(V_VTYPE, schema);
        return v != null ? v : findTypeVertex(V_ETYPE, schema);
    }

    @SuppressWarnings("unchecked")
    private static ElementSchema schemaOf(Vertex v) {
        VertexProperty<Object> p = v.property(P_SCHEMA);
        if (!p.isPresent()) return null;
        Object val = p.value();
        return val instanceof ElementSchema ? (ElementSchema) val : null;
    }

    private static boolean bool(Vertex v, String key) {
        VertexProperty<Object> p = v.property(key);
        return p.isPresent() && Boolean.TRUE.equals(p.value());
    }

    @SuppressWarnings("unchecked")
    static Set<String> labelSet(Vertex v) {
        Set<String> labels = new HashSet<>();
        v.properties(P_LABELS).forEachRemaining(p -> {
            Object val = p.value();
            if (val instanceof String) labels.add((String) val);
        });
        return labels;
    }

    @SuppressWarnings("unchecked")
    private static Set<String> propertyKeys(Vertex v) {
        Set<String> keys = new HashSet<>();
        v.properties(P_PROPERTY_KEYS).forEachRemaining(p -> {
            Object val = p.value();
            if (val instanceof String) keys.add((String) val);
        });
        return keys;
    }

    private static boolean intersects(Set<String> a, Set<String> b) {
        if (a == null || a.isEmpty() || b == null || b.isEmpty()) return false;
        for (String s : a) {
            if (b.contains(s)) return true;
        }
        return false;
    }
}
