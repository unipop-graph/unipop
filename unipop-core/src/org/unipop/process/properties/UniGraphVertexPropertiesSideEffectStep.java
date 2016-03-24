package org.unipop.process.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.structure.manager.ControllerManager;
import org.unipop.structure.manager.ControllerProvider;
import org.unipop.structure.BaseVertex;

import java.util.*;

public class UniGraphVertexPropertiesSideEffectStep extends AbstractStep<BaseVertex, BaseVertex> {

    private final int bulk;
    private ControllerManager controllerManager;
    private Iterator<Traverser<BaseVertex>> results = EmptyIterator.instance();

    public UniGraphVertexPropertiesSideEffectStep(Traversal.Admin traversal, ControllerManager controllerManager) {
        super(traversal);
        this.controllerManager = controllerManager;
        this.bulk = getTraversal().getGraph().get().configuration().getInt("bulk", 100);
    }

    @Override
    protected Traverser processNextStart() throws NoSuchElementException {
        while (!results.hasNext() && starts.hasNext())
            results = query(starts);
        if(results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser<BaseVertex>> query(ExpandableStepIterator<BaseVertex> traversers) {
        List<Traverser<BaseVertex>> copyTraversers = new ArrayList<>();
        while (traversers.hasNext() && copyTraversers.size() <= bulk)
            copyTraversers.add(traversers.next());

        if (!copyTraversers.isEmpty())
            controllerManager.getControllers(DefferedPropertiesController.class)
                    .forEach(controller -> controller.loadProperties(copyTraversers.stream().map(Traverser::get).iterator()));

        return copyTraversers.iterator();
    }
}
