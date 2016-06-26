package org.unipop.query.mutation;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

import java.util.List;

public class RemoveQuery<E extends Element> extends UniQuery {
    private final List<E> elements;

    public RemoveQuery(List<E> elements, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.elements = elements;
    }

    public List<E> getElements(){
        return elements;
    }

    public interface RemoveController extends UniQueryController {
        <E extends Element>void remove(RemoveQuery<E> uniQuery);
    }

    @Override
    public String toString() {
        return "RemoveQuery{" +
                "elements=" + elements +
                '}';
    }
}
