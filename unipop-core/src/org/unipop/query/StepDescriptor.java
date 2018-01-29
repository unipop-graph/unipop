package org.unipop.query;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Optional;
import java.util.Set;

public class StepDescriptor {

    private MutableMetrics metrics;
    private Step step;

    public StepDescriptor(Step step) {
        this.step = step;
    }

    public StepDescriptor(Step step, MutableMetrics metrics) {
        this(step);
        this.metrics = metrics;
    }

    public String getId(){
        return step.getId();
    }
    public Set<String> getLabels(){
        return step.getLabels();
    }
    public Traversal.Admin getTraversal(){
        return step.getTraversal();
    }
    public Optional<MutableMetrics> getMetrics(){
        return Optional.ofNullable(metrics);
    }

    @Override
    public String toString() {
        return step.toString()
                + " { ID: " + step.getId()
                + " }";
    }
}
