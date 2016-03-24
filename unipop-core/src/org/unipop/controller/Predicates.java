package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.*;

public class Predicates<E extends Element> {
    private Class<E> elementType;
    private Set<Object> ids;
    private ArrayList<HasContainer> hasContainers = new ArrayList<>();
    private long limitHigh = Long.MAX_VALUE;
    private Set<String> labels = new HashSet<>();

    public Predicates(Class<E> elementType, Set<String> labels, Set<Object> ids, ArrayList<HasContainer> hasContainers, long limitHigh) {
        this.elementType = elementType;
        this.ids = ids;
        this.hasContainers = hasContainers;
        this.limitHigh = limitHigh;
        this.labels = labels;
    }

    public Class<E> getElementType() {
        return elementType;
    }

    public Set<Object> getIds() {
        return ids;
    }

    public ArrayList<HasContainer> getHasContainers() {
        return hasContainers;
    }

    public long getLimitHigh() {
        return limitHigh;
    }

    public Set<String> getLabels() {
        return labels;
    }
}
