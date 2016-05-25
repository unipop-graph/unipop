package org.unipop.query;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import java.util.Set;

public class StepDescriptor {

    private Step step;

    public StepDescriptor(Step step) {
        this.step = step;
    }

    public String getId(){
        return step.getId();
    }
    public Set<String> getLabels(){
        return step.getLabels();
    }
    public MutableMetrics getMetrics(){
        return null;
        //Optional<StandardTraversalMetrics> metrics = step.getTraversal().asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY);
        //if(metrics.isPresent()) return (MutableMetrics) metrics.get().getMetrics(this.getId());
    }

    @Override
    public String toString() {
        return step.toString()
                + " { ID: " + step.getId()
                + " }";
    }
}
