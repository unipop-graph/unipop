package org.unipop.process.count;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Controller;
import org.unipop.controller.Predicates;

public interface CountController<T extends Element> extends Controller<T> {

    long count(Predicates predicates);


}
