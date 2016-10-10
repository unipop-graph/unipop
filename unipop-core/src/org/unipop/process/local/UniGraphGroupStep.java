package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.groovy.loaders.ObjectLoader;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.UniBulkStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;
import org.w3c.dom.ls.LSInput;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 10/6/16.
 */
public class UniGraphGroupStep<S, K, V> extends UniBulkStep<S, Map<K, V>> implements TraversalParent {

    private final List<LocalQuery.LocalController> controllers;
    private final List<SearchVertexQuery.SearchVertexController> nonLocalControllers;
    private UniGraphLocalStep<S, K> keyStep;
    private UniGraphLocalStep<S, V> valueStep;

    public UniGraphGroupStep(Traversal.Admin traversal, UniGraph graph, List<LocalQuery.LocalController> controllers, List<SearchVertexQuery.SearchVertexController> nonLocalControllers, GroupStep<S, K, V> groupStep) {
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
        AbstractStep<S, V> valueStepClone = valueStep.clone();
        AbstractStep<S, K> keyStepClone = keyStep.clone();
        valueStepClone.addStarts(traversers.iterator());
        keyStepClone.addStarts(traversers.iterator());
        final List<Traverser.Admin<V>>[] values = new List[]{new ArrayList<>()};
        valueStepClone.forEachRemaining(values[0]::add);
        List<Traverser.Admin<K>> keys = new ArrayList<>();
        keyStepClone.forEachRemaining(keys::add);
        Map<K, List<Traverser.Admin<V>>> map = new HashMap<>();
        keys.forEach(key -> {
            Object prev = key.getSideEffects().get("prev");
            List<Traverser.Admin> valueResult;
            List<Traverser.Admin<V>> mapValue = new ArrayList<>();
            if (prev instanceof Traverser) {
                valueResult = values[0].stream().filter(value -> ((Traverser) value.getSideEffects().get("prev")).get().equals(((Traverser) prev).get())).collect(Collectors.toList());
            } else {
                valueResult = values[0].stream().filter(value -> {
                    Object prev1 = value.getSideEffects().get("prev");
                    if (prev1 instanceof Traverser) prev1 = ((Traverser) prev1).get();
                    return prev1.equals(prev);
                }).collect(Collectors.toList());
            }
            values[0] = values[0].stream().filter(value -> {
                Object prev1 = value.getSideEffects().get("prev");
                if (prev1 instanceof Traverser) prev1 = ((Traverser) prev1).get();
                return !prev1.equals(prev);
            }).collect(Collectors.toList());
//            valueResult.forEach(v -> values.remove(v));
//            values.removeAll(valueResult);
            valueResult.stream().flatMap(o -> {
                if (o instanceof Collection)
                    return ((Collection<Traverser.Admin<V>>) o).stream();
                return Stream.<Traverser.Admin<V>>of(o);
            }).forEach(t -> mapValue.add(((Traverser.Admin<V>) t)));
            if (!map.containsKey(key.get()))
                map.put(key.get(), mapValue);
            else
                ((List) map.get(key.get())).addAll(mapValue);
        });
        Map<K, V> finalMap = new HashMap<>();
        map.entrySet().forEach((kv) -> {
            List<Traverser.Admin<V>> valueResult = kv.getValue();
            if (TraversalHelper.hasStepOfAssignableClass(CollectingBarrierStep.class, valueStep.getLocalChildren().get(0))) {
                // TODO: apply after group
                CollectingBarrierStep reducingBarrierStep = TraversalHelper.getFirstStepOfAssignableClass(CollectingBarrierStep.class, valueStep.getLocalChildren().get(0)).get();
                valueResult = runBarrier(valueResult, reducingBarrierStep);
            }
            finalMap.put(kv.getKey(), (V) valueResult.stream().map(Traverser::get).collect(Collectors.toList()));
        });

        finalMap.keySet().forEach(key -> {
            if(((List)finalMap.get(key)).size() > 0 && ((List)finalMap.get(key)).get(0) instanceof List)
                finalMap.put(key, (V) ((List<List>) finalMap.get(key)).stream().flatMap(Collection::stream).collect(Collectors.toList()));
            if(((List)finalMap.get(key)).size() > 0 && ((List)finalMap.get(key)).get(0) instanceof Map.Entry){
                Map m = new HashMap();
                ((List<Map.Entry>) finalMap.get(key)).forEach(e -> m.put(e.getKey(), e.getValue()));
                finalMap.put(key, (V)m);
            }
            if (!TraversalHelper.hasStepOfAssignableClass(FoldStep.class, valueStep.getLocalChildren().get(0))
                    && finalMap.get(key) instanceof List &&((List) finalMap.get(key)).size() == 1)
                finalMap.put(key, (V) ((List) finalMap.get(key)).get(0));
        });


        Traverser.Admin<Map<K, V>> generate = getTraversal().getTraverserGenerator().generate(finalMap, (Step<Map<K, V>, Map<K, V>>) this, 1l);
        return Collections.singleton(generate).iterator();
    }

    @Override
    protected Iterator<Traverser.Admin<Map<K, V>>> process() {
        Iterator<Traverser.Admin<Map<K, V>>> maps = super.process();
        Map<K, Object> finalMap = ConversionUtils.asStream(maps).flatMap(t -> t.get().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> {
                    if (a instanceof List){
                        if (b instanceof List)
                            ((List) a).addAll(((List) b));
                        else ((List) a).add(b);
                        return a;
                    }
                    System.out.println("a:" + a);
                    System.out.println("b:" + b);
                    return a;
                }));
        if (TraversalHelper.hasStepOfAssignableClass(CountGlobalStep.class, valueStep.getLocalChildren().get(0))) {
            finalMap.keySet().forEach(key -> {
                if (finalMap.get(key) instanceof List)
                    finalMap.put(key, (((List) finalMap.get(key)).stream().mapToLong(a -> (Long)a).sum()));
            });
        }
        if (TraversalHelper.hasStepOfAssignableClass(CollectingBarrierStep.class, valueStep.getLocalChildren().get(0))){
            CollectingBarrierStep collectingBarrierStep = TraversalHelper.getFirstStepOfAssignableClass(CollectingBarrierStep.class, valueStep.getLocalChildren().get(0)).get();
            finalMap.keySet().forEach(key -> {
                Object value = finalMap.get(key);
                if (value instanceof List){
                    List list = runBarrier(collectingBarrierStep, ((List<V>) value));
                    finalMap.put(key, list);
                }
            });
        }
        if (TraversalHelper.hasStepOfAssignableClass(SampleLocalStep.class, valueStep.getLocalChildren().get(0))){
            SampleLocalStep sampleLocalStep = TraversalHelper.getFirstStepOfAssignableClass(SampleLocalStep.class, valueStep.getLocalChildren().get(0)).get();
            SampleLocalStep<List<V>> sampleLocalStep1 = (SampleLocalStep<List<V>>) sampleLocalStep.clone();
            finalMap.keySet().forEach(key -> {
                Object value = finalMap.get(key);
                if (value instanceof List){
                    sampleLocalStep1.addStart(getAsTraversers((List<V>) value));
                    finalMap.put(key, sampleLocalStep1.next().get());
                }
            });
        }
        if (TraversalHelper.hasStepOfAssignableClass(DedupGlobalStep.class, valueStep.getLocalChildren().get(0))){
            finalMap.keySet().forEach(key -> {
                Object value = finalMap.get(key);
                if (value instanceof List){
                    finalMap.put(key, ((List) value).stream().distinct().collect(Collectors.toList()));
                }
            });
        }
        Traverser.Admin<Map<K, V>> generate = getTraversal().getTraverserGenerator().generate((Map<K,V>)finalMap, (Step<Map<K, V>, Map<K, V>>) this, 1l);
        return Collections.singleton(generate).iterator();
    }

    private List<Traverser.Admin<V>> runBarrier(List<Traverser.Admin<V>> traversers, AbstractStep<V,V> step) {
        step.reset();
        step.addStarts(traversers.iterator());
        List<Traverser.Admin<V>> results = new ArrayList<>();
        step.forEachRemaining(t -> results.add((Traverser.Admin) t));
        return results;
    }

    private List<V> runBarrier(AbstractStep<V,V> step, List<V> toTraversers){
        List<Traverser.Admin<V>> traversers = toTraversers.stream().map(t -> getTraversal().getTraverserGenerator().generate(t, (Step<V, V>) this, 1l))
                .collect(Collectors.toList());
        List<Traverser.Admin<V>> admins = runBarrier(traversers, step);
        return admins.stream().map(Traverser::get).collect(Collectors.toList());
    }

    private Traverser.Admin<List<V>> getAsTraversers(List<V> values){
        return getTraversal().getTraverserGenerator()
                .generate(values, (Step<List<V>, V>) this, 1l).asAdmin();
    }
}
