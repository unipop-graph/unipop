package org.unipop.process.start;

import org.unipop.controller.*;
import org.unipop.structure.manager.ControllerManager;
import org.unipop.structure.manager.ControllerProvider;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.helpers.StreamUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class UniGraphStartStep<S,E extends Element> extends GraphStep<S,E> {

    private final Predicates predicates;
    List<QueryController>  controllers;
    private MutableMetrics metrics;

    public UniGraphStartStep(GraphStep originalStep, Predicates predicates, ControllerManager controllerManager) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(),originalStep.getIds());
        originalStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(this::addLabel);
        this.predicates = predicates;
        Optional<StandardTraversalMetrics> metrics = this.getTraversal().asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY);
        if(metrics.isPresent()) this.metrics = (MutableMetrics) metrics.get().getMetrics(this.getId());
        this.controllers = controllerManager.getControllers(QueryController.class);
        this.setIteratorSupplier(this::query);
    }

    private Iterator<E> query() {
        return controllers.stream().map(controller -> controller.query(predicates, returnClass)).flatMap(StreamUtils::asStream).iterator();
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    public Predicates getPredicates() {
        return predicates;
    }
}
