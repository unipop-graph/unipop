package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.unipop.process.UniBulkStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 10/6/16.
 */
public abstract class UniLocalBulkStep<S,E, R>  extends UniBulkStep<S,R> implements TraversalParent {
    protected List<SearchVertexQuery.SearchVertexController> nonLocalControllers;
    protected List<LocalQuery.LocalController> controllers;
    protected List<UniGraphLocalStep<S, E>> locals;

    public UniLocalBulkStep(Traversal.Admin traversal, UniGraph graph, List<LocalQuery.LocalController> controllers, List<SearchVertexQuery.SearchVertexController> nonLocalControllers) {
        super(traversal, graph);
        this.controllers = controllers;
        this.nonLocalControllers = nonLocalControllers;
        locals = new ArrayList<>();
    }

    protected UniGraphLocalStep<S,E> createLocalStep(Traversal.Admin<S, E> traversal){
        traversal.getSteps().stream().filter(step -> step instanceof UniGraphVertexStep).forEach(step -> ((UniGraphVertexStep) step).setControllers(nonLocalControllers));
        traversal.setParent(this);
        return new UniGraphLocalStep<>(this.traversal, traversal, controllers);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return locals.stream().flatMap(p -> p.getLocalChildren().stream()).collect(Collectors.toList());
    }
}
