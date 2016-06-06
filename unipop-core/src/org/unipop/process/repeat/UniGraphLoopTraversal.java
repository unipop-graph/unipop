package org.unipop.process.repeat;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sbarzilay on 6/6/16.
 */
public class UniGraphLoopTraversal<S> extends AbstractLambdaTraversal<S, Traverser.Admin<S>> {
    private final long maxLoops;
    protected List<Traverser.Admin<S>> starts;
    private Iterator<Traverser.Admin<S>> results = null;

    public UniGraphLoopTraversal(final long maxLoops) {
        this.maxLoops = maxLoops;
        starts = new ArrayList<>();
    }

    public long getMaxLoops() {
        return maxLoops;
    }

    @Override
    public void reset() {
        super.reset();
        results = null;
    }

    @Override
    public boolean hasNext() {
        if (results == null){
            results = starts.stream().filter(t -> t.loops() >= this.getMaxLoops()).iterator();
        }
        return results.hasNext();
    }

    @Override
    public Traverser.Admin<S> next() {
        return results.next();
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        this.starts.add(start);
    }

    @Override
    public String toString() {
        return "UniGraphLoops(" + this.maxLoops + ')';
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ Long.hashCode(this.maxLoops);
    }
}
