package org.unipop.query;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.process.start.UniGraphStartStep;

import java.util.Optional;
import java.util.Set;

public class StepDescriptor {

    private MutableMetrics metrics;
    private Step step;

    public StepDescriptor(Step step) {
        this.step = step;
    }

    public <S, E extends Element> StepDescriptor(Step<S, E> step, MutableMetrics metrics) {
        this(step);
        this.metrics = metrics;
    }

    public String getId(){
        return step.getId();
    }
    public Set<String> getLabels(){
        return step.getLabels();
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
