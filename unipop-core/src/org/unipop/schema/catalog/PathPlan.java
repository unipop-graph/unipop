package org.unipop.schema.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Ordered list of {@link PathHop}s forming one type-level multi-hop path.
 */
public final class PathPlan {
    private final List<PathHop> hops;

    public PathPlan(List<PathHop> hops) {
        this.hops = hops == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(hops));
    }

    public List<PathHop> getHops() {
        return hops;
    }

    public int size() {
        return hops.size();
    }

    public PathPlan append(PathHop hop) {
        List<PathHop> next = new ArrayList<>(hops);
        next.add(hop);
        return new PathPlan(next);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PathPlan)) return false;
        return hops.equals(((PathPlan) o).hops);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hops);
    }

    @Override
    public String toString() {
        return "PathPlan" + hops;
    }
}
