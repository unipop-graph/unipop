package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.process.UniBulkStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 10/31/16.
 */
public class UniGraphGroupStepRevised<S, K, V> extends UniBulkStep<S, Map<K, V>> implements TraversalParent {
    private final List<LocalQuery.LocalController> controllers;
    private final List<SearchVertexQuery.SearchVertexController> nonLocalControllers;
    private UniGraphLocalStep<S, K> keyStep;
    private UniGraphLocalStep<S, V> valueStep;

    public UniGraphGroupStepRevised(Traversal.Admin traversal, UniGraph graph,
                                    List<LocalQuery.LocalController> controllers,
                                    List<SearchVertexQuery.SearchVertexController> nonLocalControllers,
                                    GroupStep<S, K, V> groupStep) {
        super(traversal, graph);
        this.controllers = controllers;
        this.nonLocalControllers = nonLocalControllers;
        List<Traversal.Admin<?, ?>> localChildren = groupStep.getLocalChildren();
        keyStep = createLocalStep((Traversal.Admin<S, K>) localChildren.get(0));
        Traversal.Admin<S, V> svAdmin = (Traversal.Admin<S, V>) localChildren.get(1);
        svAdmin.addStep(new UnfoldStep<>(svAdmin));
        valueStep = createLocalStep(svAdmin);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    protected <T> UniGraphLocalStep<S, T> createLocalStep(Traversal.Admin<S, T> traversal) {
        traversal.getSteps().stream().filter(step -> step instanceof UniGraphVertexStep).forEach(step -> ((UniGraphVertexStep) step).setControllers(nonLocalControllers));
        traversal.setParent(this);
        return new UniGraphLocalStep<>(this.traversal, traversal, controllers);
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
        AbstractStep<S, V> valueClone = valueStep.clone();
        valueClone.addStarts(traversers.iterator());
        List<Traverser.Admin<K>> keys = new ArrayList<>();
        keyClone.forEachRemaining(keys::add);
        List<Traverser.Admin<V>> values = new ArrayList<>();
        valueClone.forEachRemaining(values::add);
        Map<Object, List<V>> prev = values.stream().collect(Collectors.groupingBy((v) -> v.getSideEffects().get("prev"),
                Collectors.collectingAndThen(
                        Collectors.toList(), (a) -> a.stream().map(Traverser::get).collect(Collectors.toList())
                )));
        Map<K, V> map = new HashMap<>();
        keys.forEach(key -> {
            if (!map.containsKey(key.get()))
                map.put(key.get(), (V) prev.get(key.getSideEffects().get("prev")));
            else {
                ArrayList value = new ArrayList<>();
                if (map.get(key.get()) != null)
                    value.addAll((Collection) map.get(key.get()));
                if (prev.get(key.getSideEffects().get("prev")) != null)
                    value.addAll(prev.get(key.getSideEffects().get("prev")));
                map.put(key.get(), (V) value);
            }
        });
        Traverser.Admin<Map<K, V>> generate = traversal.getTraverserGenerator().generate(map, (Step<Map<K, V>, Map<K, V>>) this, 1);
        return Collections.singleton(generate).iterator();
    }

    private Map deepMerge(Map original, Map newMap) {
        if (original == newMap) {
            throw new IllegalStateException("Can't merge map into itself");
        }
        if (newMap != null) {
            for (Object key : newMap.keySet()) {
                if (newMap.get(key) instanceof Map && original.get(key) instanceof Map) {
                    Map originalChild = (Map) original.get(key);
                    Map newChild = (Map) newMap.get(key);
                    original.put(key, deepMerge(originalChild, newChild));
                } else {
                    if (newMap.get(key) instanceof List && original.get(key) instanceof List) {
                        List list = (List) newMap.get(key);
                        list.addAll(((List) original.get(key)));
                    } else
                        original.put(key, newMap.get(key));
                }
            }
        }
        return original;
    }

    @Override
    protected Iterator<Traverser.Admin<Map<K, V>>> process() {
        Iterator<Traverser.Admin<Map<K, V>>> proccesed = super.process();
        HashMap<K, V> finalMap = new HashMap<>();
        proccesed.forEachRemaining(map -> deepMerge(finalMap, map.get()));
        finalMap.entrySet().forEach(entry -> {
            if (entry.getValue() instanceof List) {
                List value = (List) entry.getValue();
                if (value.size() > 0) {
                    if (value.get(0) instanceof Map.Entry) {
                        Map<Object, Object> map = new HashMap();
                        value.forEach(entry1 -> {
                            Map.Entry mapEntry = (Map.Entry) entry1;
                            if (map.containsKey(mapEntry.getKey())) {
                                if (map.get(mapEntry.getKey()) instanceof List)
                                    ((List) map.get(mapEntry.getKey())).addAll(((List) mapEntry.getValue()));
                                else if (map.get(mapEntry.getKey()) instanceof Long) {
                                    map.put(mapEntry.getKey(), ((Long)map.get(( mapEntry.getKey())) + (Long) mapEntry.getValue()));
                                } else if (map.get(mapEntry.getKey()) instanceof Integer)
                                    map.put(mapEntry.getKey(), ((Integer)map.get(( mapEntry.getKey())) + (Integer) mapEntry.getValue()));
                                else
                                    throw new RuntimeException(map.get(mapEntry.getKey()).getClass() + " not supported");
                            } else
                                map.put(mapEntry.getKey(), mapEntry.getValue());
                        });
                        finalMap.put(entry.getKey(), (V) map);
                    } else if (value.size() == 1 &&
                            (!TraversalHelper.hasStepOfAssignableClass(FoldStep.class,
                                    valueStep.getLocalChildren().get(0))) &&
                            ((TraversalHelper.hasStepOfAssignableClass(ReducingBarrierStep.class,
                                    valueStep.getLocalChildren().get(0))) ||
                                    TraversalHelper.hasStepOfAssignableClass(CollectingBarrierStep.class,
                                            valueStep.getLocalChildren().get(0)) ||
                                    TraversalHelper.hasStepOfAssignableClass(ConstantStep.class,
                                            valueStep.getLocalChildren().get(0)))) {
                        finalMap.put(entry.getKey(), (V) value.get(0));
                    }
                }
            }
        });
        Traverser.Admin<Map<K, V>> generate = traversal.getTraverserGenerator().generate(finalMap, (Step<Map<K, V>, Map<K, V>>) this, 1);
        return Collections.singleton(generate).iterator();
    }
}
