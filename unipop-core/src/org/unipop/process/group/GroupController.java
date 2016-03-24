package org.unipop.process.group;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Controller;
import org.unipop.controller.Predicates;

import java.util.Map;

public interface GroupController<T extends Element> extends Controller<T> {
    Map<String, Object> groupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal);
}
