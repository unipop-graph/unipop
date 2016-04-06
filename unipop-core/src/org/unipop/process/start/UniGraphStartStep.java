package org.unipop.process.start;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.controller.*;
import org.unipop.process.predicate.ReceivesHasContainers;
import org.unipop.controller.manager.ControllerManager;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.common.util.StreamUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class UniGraphStartStep<S,E extends Element> extends GraphStep<S,E> implements ReceivesHasContainers<S, E>{

    List<QueryController>  controllers;
    private MutableMetrics metrics;
    private ArrayList<HasContainer> hasContainers = new ArrayList<>();

    public UniGraphStartStep(GraphStep<S, E> originalStep, ControllerManager controllerManager) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(),originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);
        Optional<StandardTraversalMetrics> metrics = this.getTraversal().asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY);
        if(metrics.isPresent()) this.metrics = (MutableMetrics) metrics.get().getMetrics(this.getId());
        this.controllers = controllerManager.getControllers(QueryController.class);
        this.setIteratorSupplier(this::query);
    }

    private Iterator<E> query() {
        Predicates<E> predicates = new Predicates<>(returnClass, null, this.getIds(), hasContainers, 0);
        return controllers.stream().map(controller -> controller.query(predicates)).flatMap(StreamUtils::asStream).iterator();
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return hasContainers;
    }
}
