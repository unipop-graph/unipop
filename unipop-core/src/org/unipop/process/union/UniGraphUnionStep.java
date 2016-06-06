package org.unipop.process.union;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.traverser.UniGraphTraverserStep;

import java.util.*;

/**
 * Created by sbarzilay on 6/6/16.
 */
public class UniGraphUnionStep<S,E> extends AbstractStep<S,E> {
    Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();
    List<Traversal.Admin<?, E>> unionTraversals;

    public UniGraphUnionStep(Traversal.Admin traversal, final Traversal.Admin<?, E>... unionTraversals) {
        super(traversal);
        this.unionTraversals = Arrays.asList(unionTraversals);
        this.unionTraversals.forEach(t -> t.addStep(new UniGraphTraverserStep<>(t)));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return new HashSet<TraverserRequirement>() {{
            add(TraverserRequirement.SINGLE_LOOP);
        }};
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        while (!results.hasNext() && starts.hasNext())
            results = union();

        if (results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    protected Iterator<Traverser.Admin<E>> union(){
        List<Traverser.Admin<S>> startsList = new ArrayList<>();
        List<Traverser.Admin<E>> results = new ArrayList<>();
        while (starts.hasNext()){
            startsList.add(starts.next());
        }
        this.unionTraversals.forEach(t -> {
            startsList.forEach(((Traversal.Admin<S, E>) t)::addStart);
            while(t.hasNext())
                results.add(((Traverser.Admin<E>) t.next()));
        });

        return results.iterator();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.unionTraversals);
    }
}
