package org.unipop.process.group;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.controller.ElementController;
import org.unipop.controller.Predicates;

import java.util.Map;

public interface GroupController extends ElementController {
    Map<String, Object> groupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal);
}
