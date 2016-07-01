package org.unipop.process.repeat;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.unipop.process.UniBulkStep;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by TechUser on 6/27/2016.
 */
public class UniGraphRepeatStep<S> extends UniBulkStep<S, S> implements TraversalParent {
    private Traversal.Admin<S, S> repeatTraversal = null;
    private Traversal.Admin<S, ?> untilTraversal = null;
    private Traversal.Admin<S, ?> emitTraversal = null;
    public boolean untilFirst = false;
    public boolean emitFirst = false;

    private List<Traverser.Admin<S>> emits;
    private List<Traverser.Admin<S>> untils;

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.BULK);
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        return requirements;
    }

    public UniGraphRepeatStep(RepeatStep repeatStep, Traversal.Admin traversal, UniGraph graph) {
        super(traversal, graph);
        this.emitFirst = repeatStep.emitFirst;
        this.untilFirst = repeatStep.untilFirst;
        repeatStep.getLabels().forEach(s -> this.addLabel(s.toString()));
        this.repeatTraversal = (Traversal.Admin<S, S>) repeatStep.getGlobalChildren().get(0);

        if (repeatStep.getEmitTraversal() != null) {
            emitTraversal = integrateChild(repeatStep.getEmitTraversal());
        }
        if (repeatStep.getUntilTraversal() != null) {
            untilTraversal = integrateChild(repeatStep.getUntilTraversal());
        }

        emits = new ArrayList<>();
        untils = new ArrayList<>();
    }

    public final boolean doUntil(final Traverser.Admin<S> traverser, boolean utilFirst) {
        return utilFirst == this.untilFirst && null != this.untilTraversal && TraversalUtil.test(traverser, this.untilTraversal);
    }

    public final boolean doEmit(final Traverser.Admin<S> traverser, boolean emitFirst) {
        return emitFirst == this.emitFirst && null != this.emitTraversal && TraversalUtil.test(traverser, this.emitTraversal);
    }

    @Override
    public String toString() {
        if (this.untilFirst && this.emitFirst)
            return StringFactory.stepString(this, untilString(), emitString(), this.repeatTraversal);
        else if (this.emitFirst)
            return StringFactory.stepString(this, emitString(), this.repeatTraversal, untilString());
        else if (this.untilFirst)
            return StringFactory.stepString(this, untilString(), this.repeatTraversal, emitString());
        else
            return StringFactory.stepString(this, this.repeatTraversal, untilString(), emitString());
    }

    private final String untilString() {
        return null == this.untilTraversal ? "until(false)" : "until(" + this.untilTraversal + ')';
    }

    private final String emitString() {
        return null == this.emitTraversal ? "emit(false)" : "emit(" + this.emitTraversal + ')';
    }

    @Override
    protected Iterator<Traverser.Admin<S>> process(List<Traverser.Admin<S>> traversers) {
        Iterator<Traverser.Admin<S>> iterator = traversers.iterator();
        boolean lastIter = true;
        while (true) {
            if (this.repeatTraversal.getEndStep().hasNext()) {
                return Iterators.concat(this.repeatTraversal.getEndStep(), emits.iterator());
            } else {
                if (starts.hasNext())
                    lastIter = true;
                if (!lastIter) {
                    if (emitFirst && untilFirst)
                        return emits.iterator();
                    return Iterators.concat(emits.iterator(), untils.iterator());
                }
                lastIter = false;
                while (iterator.hasNext()) {
                    Traverser.Admin<S> traverser = iterator.next();
                    if (doUntil(traverser, true)) {
                        traverser.resetLoops();
                        untils.add(traverser);
                    }
                    this.repeatTraversal.addStart(traverser);
                    if (doEmit(traverser, true)) {
                        final Traverser.Admin<S> emitSplit = traverser.split();
                        emitSplit.resetLoops();
                        emits.add(emitSplit);
                    }
                }
            }
            if (starts.hasNext())
                iterator = starts;
        }
    }

    public Traversal.Admin<S, S> getRepeatTraversal() {
        return repeatTraversal;
    }

    public Traversal.Admin<S, ?> getUntilTraversal() {
        return untilTraversal;
    }

    public Traversal.Admin<S, ?> getEmitTraversal() {
        return emitTraversal;
    }

    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        final List<Traversal.Admin<S, ?>> list = new ArrayList<>();
        if (null != this.untilTraversal)
            list.add(this.untilTraversal);
        if (null != this.emitTraversal)
            list.add(this.emitTraversal);
        return list;
    }

    public static class RepeatEndStep<S> extends ComputerAwareStep<S, S> {

        UniGraphRepeatStep<S> repeatStep;

        public RepeatEndStep(final Traversal.Admin traversal, UniGraphRepeatStep<S> repeatStep) {
            super(traversal);
            this.repeatStep = repeatStep;
        }

        @Override
        protected Iterator<Traverser.Admin<S>> standardAlgorithm() throws NoSuchElementException {
            while (true) {
                final Traverser.Admin<S> start = this.starts.next();
                start.incrLoops(this.getId());
                if (repeatStep.doUntil(start, false)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                } else {
                    if (!repeatStep.untilFirst && !repeatStep.emitFirst)
                        repeatStep.repeatTraversal.addStart(start);
                    else
                        repeatStep.addStart(start);
                    if (repeatStep.doEmit(start, false)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                }
            }
        }

        @Override
        protected Iterator<Traverser.Admin<S>> computerAlgorithm() throws NoSuchElementException {
            final RepeatStep<S> repeatStep = (RepeatStep<S>) this.getTraversal().getParent();
            final Traverser.Admin<S> start = this.starts.next();
            start.incrLoops(repeatStep.getId());
            if (repeatStep.doUntil(start, false)) {
                start.resetLoops();
                start.setStepId(repeatStep.getNextStep().getId());
                start.addLabels(repeatStep.getLabels());
                return IteratorUtils.of(start);
            } else {
                start.setStepId(repeatStep.getId());
                if (repeatStep.doEmit(start, false)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    emitSplit.setStepId(repeatStep.getNextStep().getId());
                    return IteratorUtils.of(start, emitSplit);
                }
                return IteratorUtils.of(start);
            }
        }
    }

}
