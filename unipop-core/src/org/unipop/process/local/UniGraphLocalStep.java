package org.unipop.process.local;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SampleLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.RequirementsStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.javatuples.Pair;
import org.unipop.process.UniQueryStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.StepDescriptor;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 9/4/16.
 */
public class UniGraphLocalStep<S, E> extends AbstractStep<S, E> implements TraversalParent, Profiling {

    private final Traversal.Admin<S, E> localTraversal;
    private StepDescriptor stepDescriptor;
    private List<LocalQuery.LocalController> localControllers;
    private Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();
    private UniGraph graph;
    private Iterator<Step> querySteps;
    private boolean localBarriers;

    public UniGraphLocalStep(Traversal.Admin traversal, Traversal.Admin<S, E> localTraversal,
                             List<LocalQuery.LocalController> localControllers) {
        this(traversal, localTraversal, localControllers, true);
    }

    public UniGraphLocalStep(Traversal.Admin traversal, Traversal.Admin<S, E> localTraversal,
                             List<LocalQuery.LocalController> localControllers, boolean localBarriers) {
        this((UniGraph) traversal.getGraph().get(), traversal, localTraversal, localControllers, localBarriers);
    }

    public UniGraphLocalStep(UniGraph graph, Traversal.Admin traversal, Traversal.Admin<S, E> localTraversal,
                             List<LocalQuery.LocalController> localControllers, boolean localBarriers) {
        super(traversal);
        this.graph = graph;
        this.localTraversal = localTraversal;
        this.stepDescriptor = new StepDescriptor(this);
        if (TraversalHelper.hasStepOfAssignableClass(SampleGlobalStep.class, localTraversal) ||
                TraversalHelper.hasStepOfAssignableClass(SampleLocalStep.class, localTraversal)) {
            this.localControllers = Collections.emptyList();
            TraversalHelper.getStepsOfAssignableClassRecursively(UniQueryStep.class, localTraversal).forEach(uniQueryStep -> uniQueryStep.addControllers(localControllers));
        } else
            this.localControllers = localControllers;
        this.localBarriers = localBarriers;
    }

    @Override
    public String getId() {
        return super.getId() + " local";
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.singletonList(localTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Sets.newHashSet(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (results instanceof EmptyIterator) {
            this.querySteps = localTraversal.clone().getSteps().stream().filter(s -> !(s instanceof RequirementsStep)).iterator();
            List<Traverser.Admin<S>> elements = new ArrayList<>();
            this.starts.forEachRemaining(start -> {
                Set<String> labels = new HashSet<>();
                start.path().labels().forEach(set -> set.forEach(labels::add));
                start.addLabels(Collections.singleton("orig"));
                start.setSideEffects(new DefaultTraversalSideEffects());
                start.getSideEffects().register("prev", () -> null, (a, b) -> b);
                start.getSideEffects().add("prev", start);
                elements.add(start);
            });
            Map<String, List<Traverser.Admin>> idMap;
            List<Traverser<E>> resultList = new ArrayList<>();
            List<Traverser.Admin> runElements = elements.stream().collect(Collectors.toList());
            Optional<UniQueryStep> firstStepOfAssignableClass = TraversalHelper.getFirstStepOfAssignableClass(UniQueryStep.class, localTraversal);
            if (localControllers.size() > 0 && firstStepOfAssignableClass.isPresent()) {
                while (querySteps.hasNext()) {
                    Step step = querySteps.next();
                    if (step instanceof UniQueryStep) {
                        idMap = runElements.stream().collect(Collectors.groupingBy((e) -> ((Element) e.get()).id().toString(), Collectors.toList()));
                        UniQueryStep queryStep = (UniQueryStep) step;
                        Class returnType = Edge.class;
                        if (queryStep instanceof UniGraphVertexStep) {
                            returnType = ((UniGraphVertexStep) queryStep).isReturnsVertex() ? Vertex.class : Edge.class;
                        }
                        LocalQuery localQuery = new LocalQuery(returnType, runElements.stream().map(Traverser::get).collect(Collectors.toList()), (SearchQuery) queryStep.getQuery(runElements), stepDescriptor);
                        runElements = new ArrayList<>();
                        Iterator<Iterator<Pair<String, Element>>> localResults = localControllers.stream()
                                .map(localController -> localController.<Element>local(localQuery)).iterator();
                        while (localResults.hasNext()) {
                            Iterator<Pair<String, Element>> result = localResults.next();
                            while (result.hasNext()) {
                                Pair<String, Element> pair = result.next();
                                if (idMap.containsKey(pair.getValue0())) {
                                    Iterator<Traverser.Admin> admins = idMap.get(pair.getValue0()).iterator();
                                    while (admins.hasNext()) {
                                        Traverser.Admin next = admins.next();
                                        Traverser.Admin split = next.split((E) pair.getValue1(), this);
                                        Traverser.Admin traverser = split;
                                            traverser.setSideEffects(new DefaultTraversalSideEffects());
                                            traverser.getSideEffects().register("prev", () -> null, (a, b) -> b);
                                            traverser.getSideEffects().add("prev", next.path("orig"));
                                            traverser.addLabels(step.getLabels());
                                            runElements.add(traverser);
                                    }
                                }
                            }
                        }
                    } else {
                        Map<Object, List<Traverser.Admin>> traversers = runElements.stream()
                                .collect(Collectors.groupingBy(
                                        t -> (t.getSideEffects().get("prev") instanceof Traverser ? ((Traverser) t.getSideEffects().get("prev")).get() : t.getSideEffects().get("prev")),
                                        Collectors.toList()));
                        elements.stream().filter(e -> !traversers.containsKey(e.get())).forEach(e -> traversers.put(e.get(), Collections.emptyList()));
                        Set<Map.Entry<Object, List<Traverser.Admin>>> traverserEntries = traversers.entrySet();
                        runElements.clear();
                        for (Map.Entry<Object, List<Traverser.Admin>> traverserEntry : traverserEntries) {
                            if ((!(step instanceof Barrier)) || localBarriers) {
                                step.reset();
                            }
                            step.addStarts(traverserEntry.getValue().iterator());
                            if ((!(step instanceof Barrier)) || localBarriers) {
                                while (step.hasNext()) {
                                    Traverser.Admin next = (Traverser.Admin) step.next();
                                    next.setSideEffects(new DefaultTraversalSideEffects());
                                    next.getSideEffects().register("prev", () -> null, (a, b) -> b);
                                    next.getSideEffects().add("prev", traverserEntry.getKey());
                                    runElements.add(next);
                                }
                            }
                        }
                        if (!localBarriers && (step instanceof Barrier)){
                            while (step.hasNext()) {
                                Traverser.Admin next = (Traverser.Admin) step.next();
                                runElements.add(next);
                            }
                        }
                    }
                }
            } else {
                runElements.clear();
            }
            runElements.forEach(resultList::add);
            if (localTraversal instanceof ElementValueTraversal ||
                    localTraversal instanceof IdentityTraversal ||
                    localTraversal instanceof TokenTraversal) {
                resultList.clear();
                for (Traverser.Admin<S> element : elements) {
                    localTraversal.reset();
                    localTraversal.addStart(element);
                    Traverser.Admin<E> split = element.split(localTraversal.next(), this);
                    split.setSideEffects(new DefaultTraversalSideEffects());
                    split.getSideEffects().register("prev", () -> null, (a, b) -> b);
                    split.getSideEffects().add("prev", element.path("orig"));
                    resultList.add(split);
                }
            } else if (firstStepOfAssignableClass.isPresent()) {
                if (firstStepOfAssignableClass.get().hasControllers()) {
                    for (Traverser.Admin<S> element : elements) {
                        localTraversal.reset();
                        localTraversal.addStart(element);
                        while (localTraversal.getEndStep().hasNext()) {
                            Traverser.Admin<E> next = localTraversal.getEndStep().next();
                            next.setSideEffects(new DefaultTraversalSideEffects());
                            next.getSideEffects().register("prev", () -> null, (a, b) -> b);
                            next.getSideEffects().add("prev", element.path("orig"));
                            resultList.add(next);
                        }
                    }
                }
            }
            else{
                for (Traverser.Admin<S> element : elements) {
                    if (localBarriers)
                        localTraversal.reset();
                    localTraversal.addStart(element);
                    if (localBarriers)
                        while (localTraversal.getEndStep().hasNext()) {
                            Traverser.Admin<E> next = localTraversal.getEndStep().next();
                            next.setSideEffects(new DefaultTraversalSideEffects());
                            next.getSideEffects().register("prev", () -> null, (a, b) -> b);
                            next.getSideEffects().add("prev", element.path("orig"));
                            resultList.add(next);
                        }
                }
                if (!localBarriers){
                    while (localTraversal.getEndStep().hasNext()) {
                        Traverser.Admin<E> next = localTraversal.getEndStep().next();
                        resultList.add(next);
                    }
                }
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
        Traversal.Admin<S, E> clone = localTraversal.clone();
        clone.reset();
        elements.forEach(clone::addStart);
        return clone.getSteps().stream().filter(step -> step instanceof UniQueryStep)
                .map(step -> ((UniQueryStep) step).getQuery(elements)).map(query -> ((SearchQuery) query)).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.localTraversal);
    }
}
