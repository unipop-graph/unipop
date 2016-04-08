package org.unipop.query;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import java.util.Optional;
import java.util.Set;

public class StepDescriptor {

    private final String id;
    private final Set<String> labels;
    private MutableMetrics metrics;

    public StepDescriptor(Step step) {
        this.id = step.getId();
        this.labels = step.getLabels();
        Optional<StandardTraversalMetrics> metrics = step.getTraversal().asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY);
        if(metrics.isPresent()) this.metrics = (MutableMetrics) metrics.get().getMetrics(this.getId());
    }

    public String getId(){
        return id;
    }
    public Set<String> getLabels(){
        return labels;
    }
    public MutableMetrics getMetrics(){
        return metrics;
    }
}
