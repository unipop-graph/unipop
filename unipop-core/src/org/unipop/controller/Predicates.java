package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.*;

public class Predicates<E extends Element> {
    private Class<E> elementType;
    private Object[] ids;
    private List<HasContainer> hasContainers;
    private long limitHigh = Long.MAX_VALUE;
    private String[] labels;

    public Predicates(Class<E> elementType, String[] labels, Object[] ids, List<HasContainer> hasContainers, long limitHigh) {
        this.elementType = elementType;
        this.ids = ids;
        this.hasContainers = hasContainers;
        this.limitHigh = limitHigh;
        this.labels = labels;
    }

    public Class<E> getElementType() {
        return elementType;
    }

    public Object[] getIds() {
        return ids;
    }

    public List<HasContainer> getHasContainers() {
        return hasContainers;
    }

    public long getLimitHigh() {
        return limitHigh;
    }

    public String[] getLabels() {
        return labels;
    }
}
