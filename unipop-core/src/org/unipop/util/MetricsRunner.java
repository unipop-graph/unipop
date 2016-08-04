package org.unipop.util;

import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.SimpleController;
import org.unipop.schema.element.ElementSchema;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/2/16.
 */
public class MetricsRunner {
    private Optional<MutableMetrics> metrics;
    private MutableMetrics controllerMetrics;

    public MetricsRunner(SimpleController controller, UniQuery query,
                         Collection<ElementSchema> schemas) {
        if (query.getStepDescriptor() != null) {
            this.metrics = query.getStepDescriptor().getMetrics();
            this.controllerMetrics = new MutableMetrics(query.getStepDescriptor().getId() + controller.toString(), controller.toString());
        }
        else {
            this.metrics = Optional.empty();
            this.controllerMetrics = new MutableMetrics(controller.toString(), controller.toString());
        }
        metrics.ifPresent(metric -> metric.addNested(controllerMetrics));
        controllerMetrics.start();
        List<MutableMetrics> childMetrics = schemas.stream().map((schema) -> new MutableMetrics(controllerMetrics.getId() + schema.toString(), schema.toString())).collect(Collectors.toList());
        childMetrics.forEach(controllerMetrics::addNested);
    }

    @FunctionalInterface
    public interface FillChildren{
        void fillChildren(List<MutableMetrics> children);
    }

    public void stop(FillChildren fillChildren) {
        controllerMetrics.stop();
        if (metrics.isPresent()) {
            fillChildren.fillChildren(controllerMetrics.getNested().stream().map(m -> ((MutableMetrics) m)).collect(Collectors.toList()));
            controllerMetrics.setCount(TraversalMetrics.ELEMENT_COUNT_ID,
                    controllerMetrics.getNested().stream()
                            .mapToLong(n -> n.getCount(TraversalMetrics.ELEMENT_COUNT_ID)).sum());
        }
    }
}
