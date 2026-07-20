package org.unipop.query.search;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Multi-hop adjacency query: collapse a 2-hop {@code out}/{@code in} chain into one backend
 * request when possible. Carriers are adjacencyJoinDirected edges (shell source = start,
 * hydrated in = final vertex). Controllers return {@code null} to signal sequential fallback.
 */
public class MultiHopQuery extends UniQuery {

    public static final class HopSpec {
        private final Direction direction;
        private final PredicatesHolder edgePredicates;

        public HopSpec(Direction direction, PredicatesHolder edgePredicates) {
            this.direction = direction == null ? Direction.OUT : direction;
            this.edgePredicates = edgePredicates == null ? PredicatesHolderFactory.empty() : edgePredicates;
        }

        public Direction getDirection() {
            return direction;
        }

        public PredicatesHolder getEdgePredicates() {
            return edgePredicates;
        }
    }

    private final List<Vertex> starts;
    private final List<HopSpec> hops;
    private final PredicatesHolder finalTargetPredicates;
    private final List<Pair<String, Order>> finalOrders;
    private final int finalLimit;
    private final Set<String> propertyKeys;

    public MultiHopQuery(List<Vertex> starts,
                         List<HopSpec> hops,
                         PredicatesHolder finalTargetPredicates,
                         List<Pair<String, Order>> finalOrders,
                         int finalLimit,
                         Set<String> propertyKeys,
                         StepDescriptor stepDescriptor) {
        super(stepDescriptor);
        this.starts = starts == null ? Collections.emptyList() : starts;
        this.hops = hops == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(hops));
        this.finalTargetPredicates = finalTargetPredicates == null ? PredicatesHolderFactory.empty() : finalTargetPredicates;
        this.finalOrders = finalOrders;
        this.finalLimit = finalLimit;
        this.propertyKeys = propertyKeys;
    }

    public List<Vertex> getStarts() {
        return starts;
    }

    public List<HopSpec> getHops() {
        return hops;
    }

    public PredicatesHolder getFinalTargetPredicates() {
        return finalTargetPredicates;
    }

    public List<Pair<String, Order>> getFinalOrders() {
        return finalOrders;
    }

    public int getFinalLimit() {
        return finalLimit;
    }

    public Set<String> getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * Multi-hop capable controllers. Returning {@code null} means "not applicable — fall back to
     * sequential single-hop execution".
     */
    public interface MultiHopController extends UniQueryController {
        Iterator<Edge> search(MultiHopQuery query);
    }
}
