package org.unipop.process.bulk;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.common.util.ConversionUtils;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by sbarzilay on 6/24/16.
 */
public abstract class UniBulkStep<S, E> extends AbstractStep<S, E> {
    protected final int bulk;
    protected Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();

    public UniBulkStep(Traversal.Admin traversal, UniGraph graph) {
        super(traversal);
        this.bulk = graph.configuration().getInt("bulk", 100);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (!results.hasNext() && starts.hasNext())
            results = query();

        if (results.hasNext()) {
            return results.next();
        }

        throw FastNoSuchElementException.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.results = EmptyIterator.instance();
    }

    private Iterator<Traverser.Admin<E>> query() {
        UnmodifiableIterator<List<Traverser.Admin<S>>> partitionedTraversers = Iterators.partition(starts, bulk);
        return ConversionUtils.asStream(partitionedTraversers)
                .<Iterator<Traverser.Admin<E>>>map(this::stepLogic)
                .<Traverser.Admin<E>>flatMap(ConversionUtils::asStream).iterator();
    }

    protected abstract Iterator<Traverser.Admin<E>> stepLogic(List<Traverser.Admin<S>> traversers);
}
