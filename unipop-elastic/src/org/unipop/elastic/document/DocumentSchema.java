package org.unipop.elastic.document;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;

public interface DocumentSchema<E extends Element> extends ElementSchema<E>{

    IndexPropertySchema getIndex();
    QueryBuilder getSearch(SearchQuery<E> query);
    List<E> parseResults(String result, PredicateQuery query);

    BulkableAction<DocumentResult> addElement(E element, boolean create);
    Delete.Builder delete(E element);
}
