package org.unipop.process.vertex;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.process.edge.EdgeStepsStrategy;
import org.unipop.process.graph.UniGraphStepStrategy;
import org.unipop.process.predicate.PredicatesUtil;
import org.unipop.query.controller.SupportsUnboundedAdjacency;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniGraph;

import java.util.Set;

/**
 * Rewrites an unbounded {@code g.V().out(L)} / {@code in(L)} / {@code both(L)} into the equivalent
 * {@code g.E().hasLabel(L).inV()} / {@code outV()} / {@code bothV()} so the traversal starts by
 * scanning the edge store directly instead of enumerating every source vertex only to use its id as
 * an adjacency seed. The rewrite is a pure step-shape transformation on the native TinkerPop steps;
 * it runs before {@link UniGraphStepStrategy} / {@link UniGraphVertexStepStrategy} /
 * {@link EdgeStepsStrategy}, which then convert the injected {@code E()} + edge-vertex step to the
 * Uni steps and fold the trailing {@code has()} into the produced-vertex fetch as usual. The result
 * multiset is identical to the original (each edge contributes its neighbour exactly as {@code out()}
 * over all vertices does).
 *
 * <p>Opt-in via {@code unipop.unboundedAdjacencyScan} (default off). It only fires when the source
 * vertex is a pure seed — no ids ({@code g.V()} is unbounded), no step label on the source, and the
 * traversal needs neither PATH, LABELED_PATH nor SACK — because those would reference the source
 * vertex the rewrite discards. Any miss falls back to the unchanged path.
 */
public class UnboundedVertexAdjacencyStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        // Must run before these so they adopt the E() + edge-vertex step we inject
        // (applyPost = strategies that run AFTER this one).
        return Sets.newHashSet(UniGraphStepStrategy.class, UniGraphVertexStepStrategy.class, EdgeStepsStrategy.class);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) return;

        Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof UniGraph)) return;
        UniGraph uniGraph = (UniGraph) graph;
        if (!uniGraph.configuration().getBoolean("unipop.unboundedAdjacencyScan", false)) return;

        // The source vertex would leak through any of these; if so, keep it (fall back entirely).
        Set<TraverserRequirement> reqs = traversal.getTraverserRequirements();
        if (reqs.contains(TraverserRequirement.PATH)
                || reqs.contains(TraverserRequirement.LABELED_PATH)
                || reqs.contains(TraverserRequirement.SACK)) return;

        // If a search controller can honour an unbounded-source SearchVertexQuery, fold the whole
        // hop into a single edge-JOIN start step; otherwise use the backend-agnostic g.E().<adj>()
        // rewrite (which reaches the same result via an edge scan + deferred target fetch).
        boolean joinCapable = uniGraph.getControllerManager()
                .getControllers(SearchVertexQuery.SearchVertexController.class).stream()
                .anyMatch(c -> c instanceof SupportsUnboundedAdjacency);

        for (GraphStep gs : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            if (!gs.isStartStep()) continue;
            if (!Vertex.class.isAssignableFrom(gs.getReturnClass())) continue;
            if (gs.getIds().length != 0) continue;          // bounded g.V(id...) -> keep
            if (!gs.getLabels().isEmpty()) continue;        // labeled source (as()) may be select()ed -> keep

            Step<?, ?> next = gs.getNextStep();
            if (!(next instanceof VertexStep)) continue;    // a has()/other step between V() and the hop -> keep
            VertexStep<?> vs = (VertexStep<?>) next;
            if (!vs.returnsVertex()) continue;              // outE()/inE()/bothE() -> keep (v1: vertex hops only)

            if (joinCapable) {
                // Single edge-JOIN start step; collectVertexPredicates folds the trailing has() into
                // its target predicates (edge labels come from the VertexStep in the step's ctor).
                UniGraphUnboundedAdjacencyStep joinStep = new UniGraphUnboundedAdjacencyStep(vs, uniGraph, uniGraph.getControllerManager());
                traversal.removeStep(gs);
                TraversalHelper.replaceStep((Step) vs, (Step) joinStep, traversal);
                joinStep.setVertexPredicates(PredicatesUtil.collectVertexPredicates(joinStep, traversal));
                continue;
            }

            // Backend-agnostic rewrite: out() neighbour is the edge's IN vertex; in() the OUT; both() -> both.
            Direction hop = vs.getDirection();
            Direction edgeVertexDir = hop.equals(Direction.OUT) ? Direction.IN
                    : hop.equals(Direction.IN) ? Direction.OUT : Direction.BOTH;
            String[] edgeLabels = vs.getEdgeLabels();

            GraphStep edgeStep = new GraphStep(traversal, Edge.class, true);
            EdgeVertexStep edgeVertexStep = new EdgeVertexStep(traversal, edgeVertexDir);
            vs.getLabels().forEach(edgeVertexStep::addLabel);

            TraversalHelper.replaceStep((Step) gs, (Step) edgeStep, traversal);
            TraversalHelper.replaceStep((Step) vs, (Step) edgeVertexStep, traversal);
            if (edgeLabels.length > 0) {
                HasStep labelHas = new HasStep(traversal, new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));
                TraversalHelper.insertAfterStep(labelHas, edgeStep, traversal);
            }
        }
    }
}
