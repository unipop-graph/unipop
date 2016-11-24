package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.UniBulkStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by sbarzilay on 10/31/16.
 */
public class UniGraphGroupStep<S, K, V> extends UniBulkStep<S, Map<K, V>> implements TraversalParent, Barrier<Map<K,V>> {
    private final List<LocalQuery.LocalController> controllers;
    private final List<SearchVertexQuery.SearchVertexController> nonLocalControllers;
    private UniGraphLocalStep<S, K> keyStep;
    private UniGraphLocalStep<S, V> valueStep;
    private Map<K, UniGraphLocalStep<S, V>> result;

    public UniGraphGroupStep(Traversal.Admin traversal, UniGraph graph,
                             List<LocalQuery.LocalController> controllers,
                             List<SearchVertexQuery.SearchVertexController> nonLocalControllers,
                             GroupStep<S, K, V> groupStep) {
        super(traversal, graph);
        this.controllers = controllers;
        this.nonLocalControllers = nonLocalControllers;
        List<Traversal.Admin<?, ?>> localChildren = groupStep.getLocalChildren();
        keyStep = createLocalStep((Traversal.Admin<S, K>) localChildren.get(0), true);
        Traversal.Admin<S, V> svAdmin = (Traversal.Admin<S, V>) localChildren.get(1);
        valueStep = createLocalStep(svAdmin, false);
        result = new HashMap<>();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    protected <T> UniGraphLocalStep<S, T> createLocalStep(Traversal.Admin<S, T> traversal, boolean localBarriers) {
        traversal.getSteps().stream().filter(step -> step instanceof UniGraphVertexStep).forEach(step -> ((UniGraphVertexStep) step).setControllers(nonLocalControllers));
        traversal.setParent(this);
        return new UniGraphLocalStep<>(this.traversal, traversal, controllers, localBarriers);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>();
        children.addAll(keyStep.getLocalChildren());
        children.addAll(valueStep.getLocalChildren());
        return children;
    }

    @Override
    protected Iterator<Traverser.Admin<Map<K, V>>> process(List<Traverser.Admin<S>> traversers) {
        AbstractStep<S, K> keyClone = keyStep.clone();
        keyClone.addStarts(traversers.iterator());
        keyClone.forEachRemaining(key -> {
            if (!result.containsKey(key.get()))
                result.put(key.get(), (UniGraphLocalStep<S, V>) valueStep.clone());
            Object prev = key.getSideEffects().get("prev");
            traversers.forEach(traverser -> {
                if (prev instanceof List) {
                    for (Object p : ((List) prev)) {
                        if (p.equals(traverser.get())){
                            result.get(key.get()).addStart(traverser);
                            break;
                        }
                    }
                } else if (prev.equals(traverser.get()))
                    result.get(key.get()).addStart(traverser);
            });
        });
        result.values().forEach(t -> {
            Step<?, V> endStep = t.getLocalChildren().get(0).getEndStep();
            if (endStep instanceof Barrier)
                ((Barrier) endStep).processAllStarts();
        });
        return EmptyIterator.instance();
    }

    @Override
    protected Iterator<Traverser.Admin<Map<K, V>>> process() {
        Iterator<Traverser.Admin<Map<K, V>>> proccesed = super.process();
        while (proccesed.hasNext())
            proccesed.next();
        Map<K, V> map = new HashMap<>();
        result.entrySet().forEach(kv -> {
            kv.getValue().softReset();
            if (kv.getValue().hasNext())
                map.put(kv.getKey(), (V) kv.getValue().next().get());
        });
        if (map.isEmpty())
            throw FastNoSuchElementException.instance();
        Traverser.Admin<Map<K, V>> generate = traversal.getTraverserGenerator().generate(map, (Step<Map<K, V>, Map<K, V>>) this, 1);
        return Collections.singleton(generate).iterator();
    }

    @Override
    public void processAllStarts() {

    }

    @Override
    public boolean hasNextBarrier() {
        return false;
    }

    @Override
    public Map<K, V> nextBarrier() throws NoSuchElementException {
        return null;
    }

    @Override
    public void addBarrier(Map<K, V> barrier) {

    }

    @Override
    public MemoryComputeKey<Map<K, V>> getMemoryComputeKey() {
        return null;
    }
}
