package org.unipop.query.mutation;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.Map;

public class AddVertexQuery extends UniQuery{
    private final Map<String, Object> properties;

    public AddVertexQuery(Map<String, Object> properties, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.properties = properties;
    }

    public Map<String, Object>  getProperties() {
        return properties;
    }

    public interface AddVertexController extends UniQueryController {
        Vertex addVertex(AddVertexQuery uniQuery);
    }

    @Override
    public String toString() {
        return "AddVertexQuery{" +
                "properties=" + properties +
                '}';
    }
}
