package org.apache.tinkerpop.gremlin.elastic.elasticservice;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;

public class Predicates {
    public ArrayList<HasContainer> hasContainers = new ArrayList<>();
    public long limitLow;
    public long limitHigh = 2000000;
    public ArrayList<String> labels = new ArrayList<>();
}
