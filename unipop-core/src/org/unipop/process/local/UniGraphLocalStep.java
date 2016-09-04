package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.UniQueryStep;
import org.unipop.query.StepDescriptor;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 9/4/16.
 */
public class UniGraphLocalStep<S extends Element, E> extends AbstractStep<S, E> implements Profiling {

    private final Traversal.Admin<S, Traverser<E>> localTraversal;
    private StepDescriptor stepDescriptor;
    private List<LocalQuery.LocalController> localControllers;
    private Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();
    private UniGraph graph;

    public UniGraphLocalStep(Traversal.Admin traversal, Traversal.Admin<S, Traverser<E>> localTraversal,
                             List<LocalQuery.LocalController> localControllers) {
        super(traversal);
        this.graph = (UniGraph) this.traversal.getGraph().get();
        this.localTraversal = localTraversal;
        this.stepDescriptor = new StepDescriptor(this);
        this.localControllers = localControllers;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (results instanceof EmptyIterator) {
            List<Traverser.Admin<S>> elements = new ArrayList<>();
            this.starts.forEachRemaining(start -> elements.add((Traverser.Admin<S>) start));
            Map<String, Traverser.Admin<S>> idMap = elements.stream().collect(Collectors.toMap((e) -> ((Element) e.get()).id().toString(), (e) -> e));
            Set<SearchQuery> traversalQueries = getTraversalQueries(elements);
            List<Traverser<E>> resultList = new ArrayList<>();
            if (traversalQueries.size() == 1) {
                LocalQuery localQuery = new LocalQuery(elements.get(0).getClass(), elements.stream().map(Traverser::get).collect(Collectors.toList()), traversalQueries.iterator().next(), stepDescriptor);
                localControllers.stream()
                        .map(localController -> localController.local(localQuery))
                        .forEach(result -> ((Iterator<Object>) result).forEachRemaining(map ->
                                ((Map<String, S>) map).entrySet().forEach(entry ->
                                        resultList.add(idMap.get(entry.getKey()).split((E) entry.getValue(), this)))));
            }
            localTraversal.reset();
            elements.forEach(localTraversal::addStart);
            while (localTraversal.hasNext()) {
                resultList.add(localTraversal.next());
            }
            results = resultList.stream().map(Traverser::asAdmin).iterator();
        }
        if (results.hasNext())
            return results.next();
        throw FastNoSuchElementException.instance();
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        this.stepDescriptor = new StepDescriptor(this, metrics);
    }

    private Set<SearchQuery> getTraversalQueries(List<Traverser.Admin<S>> elements) {
        Traversal.Admin<S, Traverser<E>> clone = localTraversal.clone();
        clone.reset();
        elements.forEach(clone::addStart);
        return clone.getSteps().stream().filter(step -> step instanceof UniQueryStep)
                .map(step -> ((UniQueryStep) step).getQuery(elements)).map(query -> ((SearchQuery) query)).collect(Collectors.toSet());
    }
}
