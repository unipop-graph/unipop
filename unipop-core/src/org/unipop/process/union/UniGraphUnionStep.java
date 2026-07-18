package org.unipop.process.union;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.UniBulkStep;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Provider-level batched union. Feeds ALL input traversers (as {@code split()} copies) to each
 * branch and drains it, so a branch's own {@code UniGraphVertexStep} batches the DB query for the
 * whole input set in one round-trip, while {@code split()} preserves each input's path/labels/
 * sack/side-effects/bulk. Correctness-equivalent to native {@code UnionStep}.
 *
 * Created by sbarzilay on 6/6/16.
 */
public class UniGraphUnionStep<S,E> extends UniBulkStep<S,E> implements TraversalParent{

    /** Marker fed to branches when this union is a start step (root {@code g.union(...)}); matches
     *  native UnionStep's UNION_STARTER so starter-valued passthrough (e.g. from inject()) is dropped. */
    private static final Object UNION_STARTER = new Object();

    Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();
    List<Traversal.Admin<?, E>> unionTraversals;
    private final boolean isStart;
    private boolean first = true;

    public UniGraphUnionStep(Traversal.Admin traversal, UniGraph graph, boolean isStart, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal, graph);
        this.isStart = isStart;
        this.unionTraversals = Arrays.asList(unionTraversals);
    }

    @Override
    public  List<Traversal.Admin<S, E>> getGlobalChildren() {
        return unionTraversals.stream().map(t -> ((Traversal.Admin<S,E>) t)).collect(Collectors.toList());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.unionTraversals.stream().map(Traversal.Admin::getTraverserRequirements).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        // A root union has no incoming start; native UnionStep(isStart=true) self-seeds one starter
        // traverser, feeds it to every branch, and drops starter-valued outputs (see process()).
        if (isStart && first) {
            first = false;
            Traverser.Admin<S> starter = this.getTraversal().getTraverserGenerator().generate((S) UNION_STARTER, (Step) this, 1L);
            starter.dropPath(); // path must start from the branch outputs, not the starter marker (matches native UnionStep)
            this.addStart(starter);
        }
        return super.processNextStart();
    }

    @Override
    protected Iterator<Traverser.Admin<E>> process(List<Traverser.Admin<S>> traversers) {
        List<Traverser.Admin<E>> results = new ArrayList<>();
        for (Traversal.Admin<?, E> branch : unionTraversals) {
            branch.reset();                                              // re-accept starts across bulk batches
            traversers.forEach(t -> branch.addStart((Traverser.Admin) t.split())); // per-branch copy: keeps path/labels/sack/side-effects/bulk
            // Drain the branch's end step (traversers) rather than branch.next() (which bulk-expands a
            // bulked traverser into N objects, inflating multiplicity). This is native UnionStep's drain.
            Step<?, E> endStep = branch.getEndStep();
            while (endStep.hasNext()) {
                Traverser.Admin<E> result = endStep.next();
                if (!(isStart && UNION_STARTER == result.get())) results.add(result); // drop starter passthrough
            }
        }
        return results.iterator();
    }

    @Override
    public void reset() {
        super.reset();
        this.results = EmptyIterator.instance();
        this.first = true;
        this.unionTraversals.forEach(Traversal.Admin::reset);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.unionTraversals);
    }
}
