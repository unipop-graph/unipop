package org.unipop.controller;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Iterator;

public interface QueryController extends Controller {
     <T extends Element> Iterator<T> query(Predicates<T> predicates);
}
