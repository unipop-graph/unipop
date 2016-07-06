package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.util.ConversionUtils;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class UniBulkStep<S, E> extends AbstractStep<S, E> {
    protected final int maxBulk;
    protected int startBulk;
    protected int multiplier;
    protected Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();

    public UniBulkStep(Traversal.Admin traversal, UniGraph graph) {
        super(traversal);
        this.maxBulk = graph.configuration().getInt("bulk.max", 100);
        this.startBulk = graph.configuration().getInt("bulk.start", this.maxBulk);
        this.multiplier = graph.configuration().getInt("bulk.multiplier", 2);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (!results.hasNext() && starts.hasNext())
            results = process();

        if (results.hasNext()) {
            return results.next();
        }

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser.Admin<E>> process() {
        BulkIterator<Traverser.Admin<S>> partitionedTraversers = new BulkIterator<>(maxBulk, startBulk, multiplier, starts);
//        UnmodifiableIterator<List<Traverser.Admin<S>>> partitionedTraversers = Iterators.partition(starts, maxBulk);
        return ConversionUtils.asStream(partitionedTraversers)
                .<Iterator<Traverser.Admin<E>>>map(this::process)
                .<Traverser.Admin<E>>flatMap(ConversionUtils::asStream).iterator();
    }

    protected abstract Iterator<Traverser.Admin<E>> process(List<Traverser.Admin<S>> traversers);

    @Override
    public void reset() {
        super.reset();
        this.results = EmptyIterator.instance();
    }


}
