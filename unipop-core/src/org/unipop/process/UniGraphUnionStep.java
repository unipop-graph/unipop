package org.unipop.process;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sbarzilay on 3/23/16.
 */
public class UniGraphUnionStep<S, E> extends BranchStep<S, E, TraversalOptionParent.Pick> {
    private boolean first = true;

    public UniGraphUnionStep(final Traversal.Admin traversal, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal);
        this.setBranchTraversal(new ConstantTraversal<>(Pick.any));
        for (final Traversal.Admin<?, E> union : unionTraversals) {
            this.addGlobalChildOption(Pick.any, (Traversal.Admin) union);
        }
    }

    @Override
    public void addGlobalChildOption(final Pick pickToken, final Traversal.Admin<S, E> traversalOption) {
        if (Pick.any != pickToken)
            throw new IllegalArgumentException("Union step only supports the any token: " + pickToken);
        super.addGlobalChildOption(pickToken, traversalOption);
    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            if (!this.first) {
                for (final List<Traversal.Admin<S, E>> options : this.traversalOptions.values()) {
                    for (final Traversal.Admin<S, E> option : options) {
                        if (option.hasNext())
                            return option.getEndStep();
                    }
                    throw FastNoSuchElementException.instance();
                }
            }
            if (starts.hasNext()) {
                this.first = false;
                List<Traverser.Admin<S>> startsList = IteratorUtils.toList(starts);
                final List<Traversal.Admin<S, E>> branch = traversalOptions.values().iterator().next();
                if (null != branch) {
                    branch.forEach(traversal -> {
                        traversal.reset();
                        startsList.forEach(traversal::addStart);
                    });
                }
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversalOptions.getOrDefault(Pick.any, Collections.emptyList()));
    }
}
