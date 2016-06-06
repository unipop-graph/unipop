package org.unipop.process.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.javatuples.Pair;

import java.util.NoSuchElementException;

/**
 * Created by sbarzilay on 3/30/16.
 */
public class UniGraphTraverserStep<S> extends AbstractStep<S, Traverser<S>> {
    public UniGraphTraverserStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<Traverser<S>> processNextStart() throws NoSuchElementException {
        if (starts.hasNext()) {
            Traverser.Admin<S> next = starts.next();
            return next.split(next, this);
        }
        throw FastNoSuchElementException.instance();
    }
}
