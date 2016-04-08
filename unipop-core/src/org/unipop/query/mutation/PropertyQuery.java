package org.unipop.query.mutation;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;

public class PropertyQuery extends UniQuery {
    private final Element element;
    private final Property property;
    private final Action action;

    public PropertyQuery(Element element, Property property, Action action, StepDescriptor stepDescriptor) {
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

    public Element getElement() {
        return element;
    }

    public Property getProperty(){
        return property;
    }

    public interface PropertyController extends UniQueryController {
        void property(PropertyQuery uniQuery);
    }
}
