package org.unipop.rest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;

/**
 * Created by sbarzilay on 24/11/16.
 */
public interface RestSchema<E extends Element> extends ElementSchema<E> {
    BaseRequest getSearch(SearchQuery<E> query);
    List<E> parseResults(HttpResponse<JsonNode> result, PredicateQuery query);

    BaseRequest addElement(E element);
    BaseRequest delete(E element);
}
