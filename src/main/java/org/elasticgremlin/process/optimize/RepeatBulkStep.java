package org.elasticgremlin.process.optimize;


import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class RepeatBulkStep<S> extends ComputerAwareStep<S, S> implements TraversalParent{

    public RepeatBulkStep(Traversal.Admin traversal, RepeatStep<S> originalStep)
    {
        super(traversal);
        if (null == this.repeatTraversal) this.repeatTraversal =  originalStep.getGlobalChildren().get(0);
        this.originalStep = originalStep;
    }

    private Traversal.Admin<S, S> repeatTraversal;
    private RepeatStep<S> originalStep;

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return originalStep.getRequirements();
    }

    @Override
    public String toString() {
        return "RepeatBulkStep(" + originalStep.toString() + ")";
    }

    @Override
    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return originalStep.getGlobalChildren();
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return originalStep.getLocalChildren();
    }

    @Override
    public RepeatBulkStep<S> clone() {
        return new RepeatBulkStep<>(this.traversal, this.originalStep.clone());
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.repeatTraversal.getEndStep().hasNext()) {
                return this.repeatTraversal.getEndStep();
            }
            else {
                do {
                    final Traverser.Admin<S> start = this.starts.next();
                    if (originalStep.doUntil(start, true)) {
                        start.resetLoops();
                        return IteratorUtils.of(start);
                    }
                    this.repeatTraversal.addStart(start);
                    if (originalStep.doEmit(start, true)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return  IteratorUtils.of(emitSplit);
                    }
                } while(this.starts.hasNext());
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        final Traverser.Admin<S> start = this.starts.next();
        if (originalStep.doUntil(start, true)) {
            start.resetLoops();
            start.setStepId(this.getNextStep().getId());
            return IteratorUtils.of(start);
        } else {
            start.setStepId(this.repeatTraversal.getStartStep().getId());
            if (originalStep.doEmit(start, true)) {
                final Traverser.Admin<S> emitSplit = start.split();
                emitSplit.resetLoops();
                emitSplit.setStepId(this.getNextStep().getId());
                return IteratorUtils.of(start, emitSplit);
            } else {
                return IteratorUtils.of(start);
            }
        }
    }
}