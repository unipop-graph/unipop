package com.tinkerpop.gremlin.elastic.process.graph.traversal.traversalHolder;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.*;

/**
 * Created by Eliran on 20/3/2015.
 */
public class ElasticUnionStep<S,E> extends ComputerAwareStep<S, E> implements TraversalHolder<S, E> {
    private UnionStep originalStep;
    public ElasticUnionStep(Traversal traversal,UnionStep originalStep) {
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

}
