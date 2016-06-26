package org.unipop.process.union;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.bulk.UniBulkStep;
import org.unipop.process.traverser.UniGraphTraverserStep;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 6/6/16.
 */
public class UniGraphUnionStep<S,E> extends UniBulkStep<S,E> {
    Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();
    List<Traversal.Admin<?, E>> unionTraversals;

    public UniGraphUnionStep(Traversal.Admin traversal, UniGraph graph, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal, graph);
        this.unionTraversals = Arrays.asList(unionTraversals);
        this.unionTraversals.forEach(t -> t.addStep(new UniGraphTraverserStep<>(t)));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.unionTraversals.stream().map(Traversal.Admin::getTraverserRequirements).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    protected Iterator<Traverser.Admin<E>> process(List<Traverser.Admin<S>> traversers) {
        List<Traverser.Admin<E>> results = new ArrayList<>();
        this.unionTraversals.forEach(t->{
            traversers.forEach(((Traversal.Admin<S, E>) t)::addStart);
            while(t.hasNext())
                results.add((Traverser.Admin<E>) t.next());
        });
        return results.iterator();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.unionTraversals);
    }
}
