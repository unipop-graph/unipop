package org.unipop.schema.catalog;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * One hop in a multi-hop type-level path request (direction + optional closed label filters).
 */
public final class Hop {
    private final Direction direction;
    private final Set<String> edgeLabels;
    private final Set<String> targetLabels;

    public Hop(Direction direction, Set<String> edgeLabels, Set<String> targetLabels) {
        this.direction = direction == null ? Direction.OUT : direction;
        this.edgeLabels = edgeLabels == null || edgeLabels.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(edgeLabels));
        this.targetLabels = targetLabels == null || targetLabels.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(targetLabels));
    }

    public static Hop of(Direction direction) {
        return new Hop(direction, null, null);
    }

    public static Hop of(Direction direction, Set<String> edgeLabels) {
        return new Hop(direction, edgeLabels, null);
    }

    public static Hop of(Direction direction, Set<String> edgeLabels, Set<String> targetLabels) {
        return new Hop(direction, edgeLabels, targetLabels);
    }

    public Direction getDirection() {
        return direction;
    }

    public Set<String> getEdgeLabels() {
        return edgeLabels;
    }

    public Set<String> getTargetLabels() {
        return targetLabels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Hop)) return false;
        Hop hop = (Hop) o;
        return direction == hop.direction
                && edgeLabels.equals(hop.edgeLabels)
                && targetLabels.equals(hop.targetLabels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction, edgeLabels, targetLabels);
    }

    @Override
    public String toString() {
        return "Hop{" + direction + ", edgeLabels=" + edgeLabels + ", targetLabels=" + targetLabels + '}';
    }
}
