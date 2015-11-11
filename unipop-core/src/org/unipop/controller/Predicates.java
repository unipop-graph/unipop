package org.unipop.controller;

import com.google.common.collect.FluentIterable;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.*;

public class Predicates {
    public ArrayList<HasContainer> hasContainers = new ArrayList<>();
    public long limitLow = 0;
    public long limitHigh = Long.MAX_VALUE;
    public Set<String> labels = new HashSet<>();
}
