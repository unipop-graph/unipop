package org.unipop.query.mutation;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

public class PropertyQuery<E extends Element>  extends UniQuery {
    private final E element;
    private final Property property;
    private final Action action;

    public PropertyQuery(E element, Property property, Action action, StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.element = element;
        this.property = property;
        this.action = action;
    }

    public enum Action {
        Add,
        Remove
    }

    public Action getAction(){
        return action;
    }

    public E getElement() {
        return element;
    }

    public Property getProperty(){
        return property;
    }

    public interface PropertyController extends UniQueryController {
        <E extends Element> void property(PropertyQuery<E> uniQuery);
    }

    @Override
    public String toString() {
        return "PropertyQuery{" +
                "element=" + element +
                ", property=" + property +
                ", action=" + action +
                '}';
    }
}
