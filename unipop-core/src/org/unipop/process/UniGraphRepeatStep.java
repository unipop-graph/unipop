package org.unipop.process;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 3/30/16.
 */
public class UniGraphRepeatStep<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, S> repeatTraversal = null;
    private Traversal.Admin<S, ?> untilTraversal = null;
    private Traversal.Admin<S, ?> emitTraversal = null;
    protected Iterator<Traverser<S>> results = EmptyIterator.instance();
    protected List<Traverser<S>> resultList = new ArrayList<>();
    public boolean untilFirst = false;
    public boolean emitFirst = false;

    public UniGraphRepeatStep(Traversal.Admin traversal, RepeatStep repeatStep) {
        super(traversal);
        labels = repeatStep.getLabels();
        untilTraversal = repeatStep.getUntilTraversal();
        emitTraversal = repeatStep.getEmitTraversal();
        repeatTraversal = (Traversal.Admin<S, S>) repeatStep.getGlobalChildren().get(0);
        if (emitTraversal != null)
            integrateChild(emitTraversal);
        if (untilTraversal != null)
            integrateChild(untilTraversal);
        untilFirst = repeatStep.untilFirst;
        emitFirst = repeatStep.emitFirst;
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
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.BULK);
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        return requirements;
    }

    public Traversal.Admin<S, S> getRepeatTraversal() {
        return repeatTraversal;
    }

    public final List<Traverser<S>> doUntil(final List<Traverser.Admin<S>> traversers, boolean utilFirst) {
        List<Traverser<S>> returnTraversers = new ArrayList<>();
        if (utilFirst == this.untilFirst && null != this.untilTraversal) {
            traversers.forEach(traverser -> {
                if (TraversalUtil.test(traverser, this.untilTraversal)) {
                    if (emitFirst && !untilFirst) return;
                    traverser.asAdmin().resetLoops();
                    resultList.add(traverser);
                } else returnTraversers.add(traverser);
            });
        }
        return returnTraversers;
    }

    public final void doEmit(final List<Traverser.Admin<S>> traversers, boolean emitFirst) {
        if (emitFirst == this.emitFirst && null != this.emitTraversal) {
            traversers.forEach(traverser -> {
                if (TraversalUtil.test(traverser, this.emitTraversal)) {
                    traverser.asAdmin().resetLoops();
                    resultList.add(traverser);
                }
            });
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (!results.hasNext() && starts.hasNext())
            results = standardAlgorithm();
        if (results.hasNext()) {
            return results.next();
        }
        throw FastNoSuchElementException.instance();
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.repeatTraversal.getEndStep().hasNext()) {
                return this.repeatTraversal.getEndStep();
            } else {
                if (!starts.hasNext())
                    return resultList.iterator();
                List<Traverser.Admin<S>> traversersList = new ArrayList<>();
                this.starts.forEachRemaining(traversersList::add);
                List<Traverser<S>> untils = doUntil(traversersList, true);
                if (!untils.isEmpty()) {
                    traversersList = untils.stream().map(Traverser::asAdmin).collect(Collectors.toList());
                }
                traversersList.forEach(this.repeatTraversal::addStart);
                List<Traverser.Admin<S>> splits = new ArrayList<>();
                traversersList.forEach(t -> splits.add(t.split()));
                if (untilTraversal == null || !untilFirst || !untils.isEmpty())
                    doEmit(splits, true);
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        throw new NotImplementedException();
    }

    public static class UniGraphRepeatEndStep<S> extends ComputerAwareStep<S, S> {
        UniGraphRepeatStep uniGraphRepeatStep;

        public UniGraphRepeatEndStep(final Traversal.Admin traversal, UniGraphRepeatStep uniGraphRepeatStep) {
            super(traversal);
            this.uniGraphRepeatStep = uniGraphRepeatStep;
        }

        @Override
        protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
            while (true) {
                List<Traverser.Admin<S>> traversersList = new ArrayList<>();
                this.starts.forEachRemaining(traversersList::add);
                traversersList.forEach(start -> start.incrLoops(this.getId()));
                List<Traverser<S>> untils = uniGraphRepeatStep.doUntil(traversersList, false);
                if (!untils.isEmpty()) {
                    traversersList = untils.stream().map(Traverser::asAdmin).collect(Collectors.toList());
                    List<Traverser.Admin<S>> splits = new ArrayList<>();
                    traversersList.forEach(t -> splits.add(t.split()));
                    uniGraphRepeatStep.doEmit(splits, false);
                }
                if (!uniGraphRepeatStep.untilFirst && !uniGraphRepeatStep.emitFirst)
                    traversersList.forEach(uniGraphRepeatStep::addStart);
                else
                    traversersList.forEach(uniGraphRepeatStep::addStart);

                throw FastNoSuchElementException.instance();

            }
        }

        @Override
        protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
            return null;
        }
    }
}
