package com.tinkerpop.gremlin.elastic.process.graph.traversal.traversalHolder;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by Eliran on 20/3/2015.
 */
public class ElasticRepeatStep<S> extends ComputerAwareStep<S, S> implements TraversalHolder<S, S> {

    public ElasticRepeatStep(Traversal traversal,RepeatStep<S> originalStep)
    {
        super(traversal);
        this.originalStep = originalStep;
    }

    private Step<?, S> endStep = null;
    private RepeatStep<S> originalStep;

    @Override
    public Set<TraverserRequirement> getRequirements() {
       return originalStep.getRequirements();
    }

    @Override
    public TraversalStrategies getChildStrategies() {
        return originalStep.getChildStrategies();
    }

    @SuppressWarnings("unchecked")
    public void setRepeatTraversal(final Traversal<S, S> repeatTraversal) {
        originalStep.setRepeatTraversal(repeatTraversal);
    }

    public void setUntilPredicate(final Predicate<Traverser<S>> untilPredicate) {
       originalStep.setUntilPredicate(untilPredicate);
    }

    public void setEmitPredicate(final Predicate<Traverser<S>> emitPredicate) {
        originalStep.setEmitPredicate(emitPredicate);
    }

    public List<Traversal<S, S>> getTraversals() {
        return originalStep.getTraversals();
    }

    public Predicate<Traverser<S>> getUntilPredicate() {
        return originalStep.getUntilPredicate();
    }

    public Predicate<Traverser<S>> getEmitPredicate() {
        return originalStep.getEmitPredicate();
    }

    public boolean isUntilFirst() {
        return originalStep.isUntilFirst();
    }

    public boolean isEmitFirst() {
        return originalStep.isEmitFirst();
    }

    public final boolean doUntil(final Traverser<S> traverser) {
        return originalStep.doUntil(traverser);
    }

    public final boolean doEmit(final Traverser<S> traverser) {
        return originalStep.doEmit(traverser);
    }

    @Override
    public String toString() {
        return originalStep.toString();
    }

    /////////////////////////

    @Override
    public ElasticRepeatStep<S> clone() throws CloneNotSupportedException {
        final ElasticRepeatStep<S> clone = (ElasticRepeatStep<S>) super.clone();
        return clone;
    }
    private Traversal<S,S> getRepeatTraversal(){
        return this.originalStep.getTraversals().get(0);
    }
    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        if (null == this.endStep) this.endStep = TraversalHelper.getEnd(getRepeatTraversal().asAdmin());
        ////
        while (true) {

            if (getRepeatTraversal().hasNext()) {
                final Traverser.Admin<S> start = this.endStep.next().asAdmin();
                start.incrLoops(this.getId());
                if (doUntil(start)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                } else {
                    getRepeatTraversal().asAdmin().addStart(start);
                    if (doEmit(start)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                }
            } else {
                do {
                    Traverser.Admin<S> s = this.starts.next();
                    if (this.originalStep.isUntilFirst() && doUntil(s)) {
                        s.resetLoops();
                        return IteratorUtils.of(s);
                    }
                    getRepeatTraversal().asAdmin().addStart(s);
                    if (this.originalStep.isEmitFirst() && doEmit(s)) {
                        final Traverser.Admin<S> emitSplit = s.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                }
                while(this.starts.hasNext());

            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        final Traverser.Admin<S> start = this.starts.next();
        if (this.originalStep.isUntilFirst() && doUntil(start)) {
            start.resetLoops();
            start.setStepId(this.getNextStep().getId());
            return IteratorUtils.of(start);
        } else {
            start.setStepId(TraversalHelper.getStart(getRepeatTraversal().asAdmin()).getId());
            if (this.originalStep.isEmitFirst() && doEmit(start)) {
                final Traverser.Admin<S> emitSplit = start.split();
                emitSplit.resetLoops();
                emitSplit.setStepId(this.getNextStep().getId());
                return IteratorUtils.of(start, emitSplit);
            } else {
                return IteratorUtils.of(start);
            }
        }
    }

    /////////////////////////

    public static <A, B, C extends Traversal<A, B>> C addRepeatToTraversal(final C traversal, final Traversal<B, B> repeatTraversal) {
       return RepeatStep.addRepeatToTraversal(traversal,repeatTraversal);
    }

    public static <A, B, C extends Traversal<A, B>> C addUntilToTraversal(final C traversal, final Predicate<Traverser<B>> untilPredicate) {
        return RepeatStep.addUntilToTraversal(traversal,untilPredicate);
    }

    public static <A, B, C extends Traversal<A, B>> C addEmitToTraversal(final C traversal, final Predicate<Traverser<B>> emitPredicate) {
      return RepeatStep.addEmitToTraversal(traversal,emitPredicate);
    }



}
