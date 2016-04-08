package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.search.SearchHit;
import org.unipop.query.UniQuery;
import org.unipop.common.schema.ElementSchema;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;

public interface ElasticElementSchema<E extends Element> extends ElementSchema<E> {
    String getIndex();
    <E extends Element> Filter getFilter(SearchQuery<E> uniQuery);
    default E fromFields(SearchHit hit) {
        return this.fromFields(hit.getSource());
    }
}
