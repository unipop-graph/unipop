package org.unipop.process.order;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;
import org.unipop.process.edge.EdgeStepsStrategy;
import org.unipop.process.edge.UniGraphEdgeOtherVertexStep;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.process.repeat.UniGraphRepeatStepStrategy;
import org.unipop.process.start.UniGraphStartStepStrategy;
import org.unipop.process.vertex.UniGraphVertexStepStrategy;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 8/17/16.
 */
public class UniGraphOrderStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy{
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return Sets.newHashSet(UniGraphStartStepStrategy.class, UniGraphVertexStepStrategy.class, UniGraphRepeatStepStrategy.class, EdgeStepsStrategy.class);
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, traversal).forEach(orderGlobalStep -> {
            List<Pair<Traversal.Admin, Comparator>> comparators = orderGlobalStep.getComparators();
            List<Pair<String, Order>> collect = comparators.stream()
                    .filter(pair -> pair.getValue0() instanceof ElementValueTraversal)
                    .filter(pair -> pair.getValue1() instanceof Order)
                    .map(pair -> Pair.with(((ElementValueTraversal) pair.getValue0()).getPropertyKey(),
                            ((Order) pair.getValue1())))
                    .collect(Collectors.toList());
            Collection<Orderable> orderableStepOf = getOrderableStepOf(orderGlobalStep, traversal);
            if (orderableStepOf != null && orderableStepOf.size() == 1) {
                Orderable step = orderableStepOf.iterator().next();
                step.setOrders(collect);
                Step nextStep = orderGlobalStep.getNextStep();
                if (nextStep instanceof RangeGlobalStep){
                    if (step instanceof ReceivesPredicatesHolder) {
                        RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) nextStep;
                        int limit = rangeGlobalStep.getHighRange() > Integer.MAX_VALUE ? -1 : (int) rangeGlobalStep.getHighRange();
                        ((ReceivesPredicatesHolder) step).setLimit(limit);
                    }
                }
            }
        });
    }

    private Collection<Orderable> getOrderableStepOf(Step step, Traversal.Admin<?, ?> traversal) {
        Step previous = step.getPreviousStep();
        while (!(previous instanceof Orderable)) {
            if (previous instanceof DedupGlobalStep || previous instanceof OrderGlobalStep)
                previous = previous.getPreviousStep();
            else if (previous instanceof EmptyStep) {
                TraversalParent parent = traversal.getParent();
                List<Orderable> Orderables = parent.getLocalChildren().stream().flatMap(child -> TraversalHelper.getStepsOfAssignableClassRecursively(Orderable.class, child).stream()).collect(Collectors.toList());
                if (Orderables.size() > 0)
                    previous = (Step) Orderables.get(Orderables.size() - 1);
                else
                    return null;
            } else if (previous instanceof TraversalParent) {
                List<Orderable> Orderables = Stream.concat(
                        ((TraversalParent) previous).getLocalChildren().stream(),
                        ((TraversalParent) previous).getGlobalChildren().stream())
                        .flatMap(child ->
                                TraversalHelper.getStepsOfAssignableClassRecursively(Orderable.class, child)
                                        .stream()).collect(Collectors.toList());
                if (Orderables.size() > 0)
                    return Orderables;
//                    previous = (Step) Orderables.get(Orderables.size() - 1);
                else
                    return null;
            } else if (previous instanceof WhereTraversalStep.WhereStartStep)
                return null;
            else
                previous = previous.getPreviousStep();
        }
        return Collections.singleton((Orderable) previous);
    }
}
