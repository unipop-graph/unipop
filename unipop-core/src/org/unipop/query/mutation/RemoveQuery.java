package org.unipop.query.mutation;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.List;

public class RemoveQuery extends UniQuery {
    private final List<Element> elements;

    public RemoveQuery(List<Element> elements, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.elements = elements;
    }

    public List<Element> getElements(){
        return elements;
    }

    public interface RemoveController extends UniQueryController {
        void remove(RemoveQuery uniQuery);
    }
}
