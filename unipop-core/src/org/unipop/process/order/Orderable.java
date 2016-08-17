package org.unipop.process.order;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.javatuples.Pair;

import java.util.List;

/**
 * Created by sbarzilay on 8/17/16.
 */
public interface Orderable {
    void setOrders(List<Pair<String, Order>> orders);
}
