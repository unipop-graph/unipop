package org.unipop.process;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;

/**
 * Created by sbarzilay on 3/15/16.
 */
public class UniGraphCoalesceStep<S, E> extends FlatMapStep<S, E> implements TraversalParent {
    private final List<Traversal.Admin<S, E>> coalesceTraversals;
    private Iterator<E> results = EmptyIterator.instance();
    private Traverser<S> last = null;

    public UniGraphCoalesceStep(Traversal.Admin traversal, List<Traversal.Admin<S, E>> coalesceTraversals) {
        super(traversal);
        this.coalesceTraversals = coalesceTraversals;
        this.coalesceTraversals.forEach(this::integrateChild);
    }

    @Override
    protected Traverser<E> processNextStart() {

        while (!results.hasNext() && starts.hasNext())
            results = query(starts);

        while (results.hasNext())
            return last.asAdmin().split(results.next(), this);

        throw FastNoSuchElementException.instance();
    }

    private Iterator<E> query(Iterator<Traverser.Admin<S>> traversers) {
        List<Traverser.Admin<S>> traversersList = Lists.newArrayList(traversers);
        coalesceTraversals.forEach(t -> {
            traversersList.forEach(t::addStart);
        });
        last = traversersList.get(traversersList.size() - 1);
        return flatMap(traversersList.get(0));
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<S> traverser) {
        for (final Traversal.Admin<S, E> coalesceTraversal : this.coalesceTraversals) {
            if (coalesceTraversal.hasNext())
                return coalesceTraversal;
        }
        return EmptyIterator.instance();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }
}
