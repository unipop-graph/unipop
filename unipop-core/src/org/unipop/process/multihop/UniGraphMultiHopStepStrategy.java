package org.unipop.process.multihop;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.MultiHopQuery;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Folds two consecutive <b>vertex-return</b> adjacency hops ({@code out}/{@code in}) into a
 * {@link UniGraphMultiHopStep}. Edge-final chains ({@code out().outE()}) stay as two
 * {@link UniGraphVertexStep}s — sequential single-hop joins are typically faster than an
 * unbounded multi-edge join on open topologies.
 * BOTH and intermediate labels/filters are not folded.
 */
public class UniGraphMultiHopStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphVertexStepStrategy.class);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) return;
        Graph graph = traversal.getGraph().orElse(null);
        if (!(graph instanceof UniGraph)) return;
        UniGraph uniGraph = (UniGraph) graph;

        List<Step> steps = new ArrayList<>(traversal.getSteps());
        for (int i = 0; i < steps.size() - 1; i++) {
            Step a = steps.get(i);
            Step b = steps.get(i + 1);
            if (!(a instanceof UniGraphVertexStep) || !(b instanceof UniGraphVertexStep)) continue;

            UniGraphVertexStep hop1 = (UniGraphVertexStep) a;
            UniGraphVertexStep hop2 = (UniGraphVertexStep) b;

            // Hop1 must return vertices (mid of the chain)
            if (!hop1.returnsVertex()) continue;
            // Only fold vertex-final chains (out/in → out/in).
            // Edge-final (out → outE) multi-join is often slower than sequential single-hop
            // joins on open topologies (unbounded e0⋈e1); leave as two UniGraphVertexSteps.
            if (!hop2.returnsVertex()) continue;
            if (hop1.getDirection() == Direction.BOTH || hop2.getDirection() == Direction.BOTH) continue;
            if (hop1.getNextStep() != hop2) continue;
            if (!hop1.getLabels().isEmpty() || !hop2.getLabels().isEmpty()) continue;
            // Intermediate filters/order/limit would require mid pushdown
            if (hop1.getVertexPredicates() != null && hop1.getVertexPredicates().notEmpty()) continue;
            if (hop1.getOrders() != null && !hop1.getOrders().isEmpty()) continue;
            if (hop1.getLimit() >= 0) continue;

            List<MultiHopQuery.HopSpec> hopSpecs = new ArrayList<>(2);
            hopSpecs.add(new MultiHopQuery.HopSpec(hop1.getDirection(), hop1.getPredicates()));
            hopSpecs.add(new MultiHopQuery.HopSpec(hop2.getDirection(), hop2.getPredicates()));

            PredicatesHolder finalPreds = hop2.getVertexPredicates();
            UniGraphMultiHopStep multi = new UniGraphMultiHopStep<Vertex>(
                    traversal, uniGraph, uniGraph.getControllerManager(),
                    hopSpecs, finalPreds, true);

            if (hop2.getOrders() != null) multi.setOrders(hop2.getOrders());
            if (hop2.getLimit() >= 0) multi.setLimit(hop2.getLimit());
            if (hop2.getKeys() == null) multi.fetchAllKeys();
            else for (Object key : hop2.getKeys()) multi.addPropertyKey(key.toString());

            TraversalHelper.replaceStep(hop1, multi, traversal);
            traversal.removeStep(hop2);

            steps = new ArrayList<>(traversal.getSteps());
            i = steps.indexOf(multi);
        }
    }
}
