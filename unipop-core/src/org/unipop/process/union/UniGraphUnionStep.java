package org.unipop.process.union;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.UniBulkStep;
import org.unipop.process.traverser.UniGraphTraverserStep;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 6/6/16.
 */
public class UniGraphUnionStep<S, E> extends UniBulkStep<S, E> implements TraversalParent {
    Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();
    List<Traversal.Admin<?, E>> unionTraversals;

    public UniGraphUnionStep(Traversal.Admin traversal, UniGraph graph, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal, graph);
        this.unionTraversals = Arrays.asList(unionTraversals);
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return unionTraversals.stream().map(t -> ((Traversal.Admin<S, E>) t)).collect(Collectors.toList());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.unionTraversals.stream().map(Traversal.Admin::getTraverserRequirements).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    protected Iterator<Traverser.Admin<E>> process(List<Traverser.Admin<S>> traversers) {
        List<Traverser.Admin<S>> bulkedTraversers = traversers.stream().flatMap(t -> {
            long bulk = t.bulk();
            List<Traverser.Admin<S>> adminList = new ArrayList<>();
            for (long i = 0; i < bulk; i++) {
                adminList.add(t);
            }
            return adminList.stream();
        }).collect(Collectors.groupingBy(Attachable::get)).entrySet().stream().map(entry -> {
            Traverser.Admin<S> sAdmin = entry.getValue().get(0);
            sAdmin.setBulk(entry.getValue().size());
            return sAdmin;
        }).collect(Collectors.toList());

        List<Traverser.Admin<E>> results = new ArrayList<>();
        this.unionTraversals.forEach(t -> {
            bulkedTraversers.forEach(((Traversal.Admin<S, E>) t)::addStart);
            while (t.getEndStep().hasNext()) {
                Traverser.Admin<E> next = t.getEndStep().next();
                results.add(next);
            }
        });
        return results.iterator();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.unionTraversals);
    }
}
