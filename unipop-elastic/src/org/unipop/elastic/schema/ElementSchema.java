package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.Controller;
import org.unipop.controller.Predicates;
import org.unipop.elastic.helpers.QueryIterator;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface ElementSchema<E extends Element> extends QueryIterator.Parser<E> {
    Class<E> getElementType();
    String getIndex();
    E createElement(SearchHit hit, Controller documentController);
    Map<String, Object> getFields(E element);
    Filter getFilter(Predicates predicates);
}
