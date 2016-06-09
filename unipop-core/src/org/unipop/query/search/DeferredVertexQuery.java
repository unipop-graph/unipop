package org.unipop.query.search;

import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.List;
import java.util.Set;

public class DeferredVertexQuery extends UniQuery {
    private List<DeferredVertex> vertices;
    private Set<String> propertyKeys;

    public DeferredVertexQuery(List<DeferredVertex> vertices, Set<String> propertyKeys, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.vertices = vertices;
        this.propertyKeys = propertyKeys;
    }

    public List<DeferredVertex> getVertices() {
        return this.vertices;
    }

    public Set<String> getPropertyKeys() {
        return this.propertyKeys;
    }

    public interface DefferedVertexController extends UniQueryController {
        void fetchProperties(DeferredVertexQuery query);
    }
}
