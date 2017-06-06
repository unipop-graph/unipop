package org.unipop.elastic.document;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.index.query.QueryBuilder;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.javatuples.Pair;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DocumentSchema<E extends Element> extends ElementSchema<E>{
//    String getIndex();

    IndexPropertySchema getIndex();
    QueryBuilder getSearch(SearchQuery<E> query);
    List<E> parseResults(String result, PredicateQuery query);

    BulkableAction<DocumentResult> addElement(E element, boolean create);
    Delete.Builder delete(E element);

    String getType();
}
