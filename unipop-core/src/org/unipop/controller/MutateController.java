package org.unipop.controller;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * Created by ranma on 19/03/2016.
 */
public interface MutateController extends Controller {
    void remove(Element element);
    void addProperty(Element element, Property property);
    void removeProperty(Element element, Property property);
}
