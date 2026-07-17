package org.unipop.process.range;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.graph.UniGraphStep;
import org.unipop.process.graph.UniGraphStepStrategy;
import org.unipop.process.order.UniGraphOrderStrategy;

import java.util.Set;

/** Replaces a GLOBAL RangeGlobalStep that follows a UniGraphStep with a schema-aware
 *  UniGraphRangeStep, folding the range low-bound as an offset onto the UniGraphStep so the
 *  controller can push SQL OFFSET when the query resolves to a single schema. */
public class UniGraphRangeStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStepStrategy.class, UniGraphOrderStrategy.class);
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        for (RangeGlobalStep<?> range : TraversalHelper.getStepsOfAssignableClass(RangeGlobalStep.class, traversal)) {
            UniGraphStep<?, ?> source = sourceGraphStep(range);
            if (source == null) continue;                 // not fed by a UniGraphStep -> leave native
            long low = range.getLowRange();
            long high = range.getHighRange();
            source.setOffset(low > Integer.MAX_VALUE ? 0 : (int) low);
            UniGraphRangeStep<?> uni = new UniGraphRangeStep<>(traversal, low, high, source);
            TraversalHelper.replaceStep((Step) range, uni, traversal);
        }
    }

    /** Walk back past an intervening OrderGlobalStep (also pushed to SQL, so OFFSET-after-ORDER-BY
     *  stays correct) to a UniGraphStep, else null. Does NOT skip DedupGlobalStep: dedup() is not
     *  pushed to SQL, so folding OFFSET over raw pre-dedup rows would skip the wrong rows -- a
     *  dedup() between the source and range must leave the native RangeGlobalStep in place. */
    private UniGraphStep<?, ?> sourceGraphStep(RangeGlobalStep<?> range) {
        Step<?, ?> prev = range.getPreviousStep();
        while (prev instanceof OrderGlobalStep) {
            prev = prev.getPreviousStep();
        }
        return (prev instanceof UniGraphStep) ? (UniGraphStep<?, ?>) prev : null;
    }
}
