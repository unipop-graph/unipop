package org.unipop.query.mutation;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniVertex;

import java.util.Map;

public class AddEdgeQuery extends UniQuery{
    private final Vertex outVertex;
    private final Vertex inVertex;
    private final Map<String, Object> properties;
    private final StepDescriptor stepDescriptor;

    public AddEdgeQuery(Vertex outVertex, Vertex inVertex, Map<String, Object> properties, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.properties = properties;
        this.stepDescriptor = stepDescriptor;
    }

    public Vertex getInVertex() {
        return inVertex;
    }

    public Vertex getOutVertex() {
        return outVertex;
    }

    @Override
    public StepDescriptor getStepDescriptor() {
        return stepDescriptor;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public interface AddEdgeController extends UniQueryController {
        Edge addEdge(AddEdgeQuery uniQuery);
    }

    @Override
    public String toString() {
        return "AddEdgeQuery{" +
                "outVertex=" + outVertex +
                ", inVertex=" + inVertex +
                ", properties=" + properties +
                ", stepDescriptor=" + stepDescriptor +
                '}';
    }
}
