package org.elasticgremlin.elasticservice;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;

public class Predicates {
    public ArrayList<HasContainer> hasContainers = new ArrayList<>();
    public long limitLow = 0;
    public long limitHigh = Long.MAX_VALUE;
    public ArrayList<String> labels = new ArrayList<>();
}
