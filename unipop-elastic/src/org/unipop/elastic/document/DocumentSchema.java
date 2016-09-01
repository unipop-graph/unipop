package org.unipop.elastic.document;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;
import java.util.Set;

public interface DocumentSchema<E extends Element> extends ElementSchema<E>{
//    String getIndex();

    Search getSearch(SearchQuery<E> query);
    Search getReduce(ReduceQuery query);
    Set<Object> parseReduce(String result, ReduceQuery query);
    List<E> parseResults(String result, PredicateQuery query);

    BulkableAction<DocumentResult> addElement(E element, boolean create);
    Delete.Builder delete(E element);
}
