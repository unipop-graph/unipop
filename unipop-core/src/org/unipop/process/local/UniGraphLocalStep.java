package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.RequirementsStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.javatuples.Pair;
import org.unipop.process.UniQueryStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.StepDescriptor;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniGraphTraversal;

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

    public UniGraphLocalStep(Traversal.Admin traversal, Traversal.Admin<S, E> localTraversal,
                             List<LocalQuery.LocalController> localControllers) {
        this((UniGraph) traversal.getGraph().get(), traversal, localTraversal, localControllers);
    }

    public UniGraphLocalStep(UniGraph graph, Traversal.Admin traversal, Traversal.Admin<S, E> localTraversal,
                             List<LocalQuery.LocalController> localControllers) {
        super(traversal);
        this.graph = graph;
        this.localTraversal = localTraversal;
        this.stepDescriptor = new StepDescriptor(this);
        this.localControllers = localControllers;
    }

    public Traversal<S, E> getUniTraversal(){
        UniGraphTraversal<S, E> seUniGraphTraversal = new UniGraphTraversal<>(graph);
        RequirementsStrategy.addRequirements(seUniGraphTraversal.getStrategies(), TraverserRequirement.PATH);
        seUniGraphTraversal.addStep(this);
        seUniGraphTraversal.setParent(this.getTraversal().getParent());
        return seUniGraphTraversal;
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
        return Collections.singleton(TraverserRequirement.PATH);
    }


    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (results instanceof EmptyIterator) {
            this.querySteps = localTraversal.clone().getSteps().stream().filter(s -> !(s instanceof RequirementsStep)).iterator();
            List<Traverser.Admin<S>> elements = new ArrayList<>();
            this.starts.forEachRemaining(start -> {
                Set<String> labels = new HashSet<>();
                start.path().labels().forEach(set -> set.forEach(labels::add));
                labels.add("orig");
                ((Traverser.Admin<S>) start).addLabels(labels);
                elements.add((Traverser.Admin<S>) start);
            });
            Map<String, List<Traverser.Admin>> idMap;
            List<Traverser<E>> resultList = new ArrayList<>();
            List<Traverser.Admin> runElements = elements.stream().collect(Collectors.toList());
            if (localControllers.size() > 0) {
                while (querySteps.hasNext()) {
                    Step step = querySteps.next();
                    step.setId(step.getId() + ".local");
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
                                        B_LP_O_P_S_SE_SL_Traverser split = (B_LP_O_P_S_SE_SL_Traverser) next.split((E) pair.getValue1(), this);
                                        split.setSideEffects(new DefaultTraversalSideEffects());
                                        split.getSideEffects().register("prev", ()-> null, (a,b) -> b);
                                        split.getSideEffects().add("prev", next.path("orig"));
                                        split.addLabels(step.getLabels());
                                        runElements.add(split);
                                    }
                                }
                            }
                        }
                    } else {
//                        Map<Object, List<Traverser.Admin>> traversers = runElements.stream().collect(Collectors.groupingBy(t -> ((Element) t.path("orig")).id(), Collectors.toList()));
                        Map<Object, List<Traverser.Admin>> traversers = runElements.stream()
                                .collect(Collectors.groupingBy(
                                        t -> (t.path("orig")),
                                        Collectors.toList()));
                        elements.stream().filter(e -> !traversers.containsKey(e.get())).forEach(e -> traversers.put(e.get(), Collections.emptyList()));
                        Set<Map.Entry<Object, List<Traverser.Admin>>> traverserEntries = traversers.entrySet();
                        runElements.clear();
                        for (Map.Entry<Object, List<Traverser.Admin>> traverserEntry : traverserEntries) {
                            step.reset();
                            step.addStarts(traverserEntry.getValue().iterator());
                            while (step.hasNext()) {
                                B_LP_O_P_S_SE_SL_Traverser next = (B_LP_O_P_S_SE_SL_Traverser) step.next();
                                next.setSideEffects(new DefaultTraversalSideEffects());
                                next.getSideEffects().register("prev", ()-> null, (a,b) -> b);
                                next.getSideEffects().add("prev", traverserEntry.getKey());
                                runElements.add((Traverser.Admin) next);
                            }
                        }

                    }
                }
            } else {
                runElements.clear();
            }
            runElements.forEach(resultList::add);
            Optional<UniQueryStep> firstStepOfAssignableClass = TraversalHelper.getFirstStepOfAssignableClass(UniQueryStep.class, localTraversal);
            if (localTraversal instanceof ElementValueTraversal){
                resultList.clear();
                for (Traverser.Admin<S> element : elements) {
                    localTraversal.reset();
                    localTraversal.addStart(element);
                    Traverser.Admin<E> split = element.split(localTraversal.next(), this);
                    split.setSideEffects(new DefaultTraversalSideEffects());
                    split.getSideEffects().register("prev", ()-> null, (a,b) -> b);
                    split.getSideEffects().add("prev", element.path("orig"));
                    resultList.add(split);
                }
            }
            else if (firstStepOfAssignableClass.isPresent() && firstStepOfAssignableClass.get().hasControllers()) {
                for (Traverser.Admin<S> element : elements) {
                    localTraversal.reset();
                    localTraversal.addStart(element);
                    while (localTraversal.getEndStep().hasNext()) {
                        B_LP_O_P_S_SE_SL_Traverser<E> next = (B_LP_O_P_S_SE_SL_Traverser<E>) localTraversal.getEndStep().next();
                        next.setSideEffects(new DefaultTraversalSideEffects());
                        next.getSideEffects().register("prev", ()-> null, (a,b) -> b);
                        next.getSideEffects().add("prev", element.path("orig"));
                        resultList.add((Traverser.Admin<E>) next);
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
}
