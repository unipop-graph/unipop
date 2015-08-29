package org.elasticgremlin.process.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;

import java.util.NoSuchElementException;

public class ForceFeedStep extends AbstractStep {

    public ForceFeedStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public Traverser next() {
        this.getNextStep().addStarts(this.starts);
        return EmptyTraverser.instance();
    }

    @Override
    public boolean hasNext() {
        return starts.hasNext();
    }

    @Override
    protected Traverser processNextStart() throws NoSuchElementException {
        return null;
    }
}
