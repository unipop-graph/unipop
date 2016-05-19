package org.unipop.query.search;

import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.List;

public class DeferredVertexQuery extends UniQuery {
    private List<DeferredVertex> vertices;

    public DeferredVertexQuery(List<DeferredVertex> vertices, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.vertices = vertices;
    }

    public List<DeferredVertex> getVertices() {
        return this.vertices;
    }

    public interface DefferedVertexController extends UniQueryController {
        void fetchProperties(DeferredVertexQuery query);
    }
}
