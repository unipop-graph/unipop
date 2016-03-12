package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;

import java.util.*;

/**
 * Created by sbarzilay on 3/10/16.
 */
public class UniGraphPropertiesSideEffectStep<S extends BaseElement> extends AbstractStep<S, S> {

    private final int bulk;
    private ControllerManager controllerManager;
    private Iterator<Traverser<S>> results = EmptyIterator.instance();

    public UniGraphPropertiesSideEffectStep(Traversal.Admin traversal, ControllerManager controllerManager) {
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

    private Iterator<Traverser<S>> query(ExpandableStepIterator<S> traversers) {
        List<Traverser.Admin<S>> copyTraversers = new ArrayList<>();
        ResultsContainer results = new ResultsContainer();
        List<BaseElement> elements = new ArrayList<>();
        while (traversers.hasNext() && elements.size() <= bulk){
            Traverser.Admin<S> traverser = traversers.next();
            if (traverser.get() instanceof BaseVertex)
                elements.add(traverser.get());
            copyTraversers.add(traverser);
        }

        if (!elements.isEmpty())
            results.addResults((Iterator<S>) controllerManager.properties(elements).iterator());

        List<Traverser<S>> returnTraversers = new ArrayList<>();

        copyTraversers.forEach(traverser -> {
//            ArrayList<S> list = results.get(traverser.get().id().toString());
//            if (list != null)
                returnTraversers.add(traverser);
        });

        return returnTraversers.iterator();
    }

    private class ResultsContainer {
        Map<Object, ArrayList<BaseElement>> idToResults = new HashMap<>();

        public void addResults(Iterator<S> iterator) {
            iterator.forEachRemaining(element -> {
                ArrayList<BaseElement> list = idToResults.get(element.id());
                if (list == null || !(list instanceof List)) {
                    list = new ArrayList();
                    idToResults.put(element.id(), list);
                }
                list.add(element);
            });
        }

        public ArrayList<S> get(String key) {
            return (ArrayList<S>) idToResults.get(key);
        }
    }
}
