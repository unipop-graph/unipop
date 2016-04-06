package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.Predicates;
import org.unipop.common.schema.ElementSchema;

import java.util.Map;

public interface ElasticElementSchema<E extends Element> extends ElementSchema<E> {
    String getIndex();
    Filter getFilter(Predicates<E> predicates);
    default E fromFields(SearchHit hit) {
        return this.fromFields(hit.getSource());
    }
}
