package org.unipop.query.mutation;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.Map;

public class AddEdgeQuery extends UniQuery{
    private final Vertex outVertex;
    private final Vertex inVertex;
    private final Map<String, Object> properties;

    public AddEdgeQuery(Vertex outVertex, Vertex inVertex, Map<String, Object> properties, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.properties = properties;
    }

    public Vertex getInVertex() {
        return inVertex;
    }

    public Vertex getOutVertex() {
        return outVertex;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }


    public interface AddEdgeController extends UniQueryController {
        Edge addEdge(AddEdgeQuery uniQuery);
    }
}
