package org.unipop.process.properties;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Controller;

import java.util.Iterator;

public interface DefferedPropertiesController<T extends Element> extends Controller<T> {
    void loadProperties(Iterator<T> elements);
}
