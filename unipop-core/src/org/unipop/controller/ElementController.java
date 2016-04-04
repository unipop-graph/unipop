package org.unipop.controller;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;

public interface ElementController<E extends Element> {
    Iterator<E> query(Predicates<E> predicates);
    void remove(Element element);
    void addProperty(Element element, Property property);
    void removeProperty(Element element, Property property);
}
