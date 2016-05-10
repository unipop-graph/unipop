package org.unipop.process.start;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.ControllerManager;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.common.util.StreamUtils;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;

import java.util.*;

public class UniGraphStartStep<S,E extends Element> extends GraphStep<S,E> implements ReceivesPredicatesHolder<S, E> {

    private final StepDescriptor stepDescriptor;
    List<SearchQuery.SearchController>  controllers;
    private PredicatesHolder predicates = new PredicatesHolder(PredicatesHolder.Clause.And);
    private int limit;

    public UniGraphStartStep(GraphStep<S, E> originalStep, ControllerManager controllerManager) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(),originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);
        this.stepDescriptor = new StepDescriptor(this);
        this.controllers = controllerManager.getControllers(SearchQuery.SearchController.class);
        this.setIteratorSupplier(this::query);
        limit = -1;
    }

    private Iterator<E> query() {
        SearchQuery<E> searchQuery = new SearchQuery<E>(returnClass, predicates, limit, stepDescriptor);
        return controllers.stream().<Iterator<E>>map(controller -> controller.search(searchQuery)).flatMap(StreamUtils::asStream).iterator();
    }

    @Override
    public void addPredicate(HasContainer predicatesHolder) {
        this.predicates.add(predicatesHolder);
    }

    @Override
    public PredicatesHolder getPredicates() {
        return predicates;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }
}
