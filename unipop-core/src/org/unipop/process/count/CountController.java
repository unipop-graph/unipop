package org.unipop.process.count;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.ElementController;
import org.unipop.controller.Predicates;

public interface CountController<T extends Element> extends ElementController {

    long count(Predicates predicates);


}
