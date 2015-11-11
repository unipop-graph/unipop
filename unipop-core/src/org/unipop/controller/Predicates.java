package org.unipop.controller;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Predicates {
    public ArrayList<HasContainer> hasContainers = new ArrayList<>();
    public long limitLow = 0;
    public long limitHigh = Long.MAX_VALUE;
    public Set<String> labels = new HashSet<>();


}
