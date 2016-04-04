package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.ElementController;
import org.unipop.controller.Predicates;

import java.util.Map;

public interface ElasticElementSchema<E extends Element, C extends ElementController<E>>  {
    String getIndex();
    E createElement(SearchHit hit, C documentController);
    Map<String, Object> toFields(E element);
    Filter getFilter(Predicates<E> predicates);
}
