package org.elasticgremlin.queryhandler;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;

public class Predicates {
    public ArrayList<HasContainer> hasContainers = new ArrayList<>();
    public long limitLow = 0;
    public long limitHigh = Long.MAX_VALUE;
    public ArrayList<String> labels = new ArrayList<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Predicates that = (Predicates) o;

        if (limitLow != that.limitLow) return false;
        if (limitHigh != that.limitHigh) return false;
        if (hasContainers != null ? !hasContainers.equals(that.hasContainers) : that.hasContainers != null)
            return false;
        return !(labels != null ? !labels.equals(that.labels) : that.labels != null);

    }

    @Override
    public int hashCode() {
        int result = hasContainers != null ? hasContainers.hashCode() : 0;
        result = 31 * result + (int) (limitLow ^ (limitLow >>> 32));
        result = 31 * result + (int) (limitHigh ^ (limitHigh >>> 32));
        result = 31 * result + (labels != null ? labels.hashCode() : 0);
        return result;
    }
}
