/*package org.org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.steps.traversalHolder;

import org.org.apache.tinkerpop.gremlin.process.traversal.*;
import org.org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.codehaus.groovy.antlr.treewalker.TraversalHelper;

import java.util.*;

public class ElasticUnionStep<S,E> extends ComputerAwareStep<S, E> implements TraversalHolder<S, E> {
    private UnionStep originalStep;
    public ElasticUnionStep(Traversal.Admin traversal,UnionStep originalStep) {
        super(traversal);
        this.originalStep = originalStep;
    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            for (final Traversal<S, E> union : this.getTraversals()) {
                if (union.hasNext()) return TraversalHelper.getEnd(union.asAdmin());
            }
            do {
                Traverser.Admin<S> next = this.starts.next();
                this.getTraversals().forEach(union -> union.asAdmin().addStart(next.split()));
            }
            while (this.starts.hasNext());

        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() throws NoSuchElementException {
        return null;
    }

    @Override
    public List<Traversal<S, E>> getTraversals() {
        return originalStep.getTraversals();
    }

    @Override
    public TraversalStrategies getChildStrategies() {
        return originalStep.getChildStrategies();
    }


    @Override
    public String toString() {
        return
                "Elastic"+originalStep.toString();
    }

    @Override
    public ElasticUnionStep<S, E> clone() throws CloneNotSupportedException {
        final ElasticUnionStep<S, E> clone = (ElasticUnionStep<S, E>) super.clone();
        return clone;
    }

    @Override
    public void reset() {
       originalStep.reset();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return originalStep.getTraversalRequirements();
    }

}*/
