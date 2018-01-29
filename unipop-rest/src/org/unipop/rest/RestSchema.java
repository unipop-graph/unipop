package org.unipop.rest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * A schema that is represented by data in a rest service.
 * @param <E> Element
 */
public interface RestSchema<E extends Element> extends ElementSchema<E> {

    /**
     * Converts a search query to a HTTP request
     * @param query The search query
     * @return A HTTP request
     */
    BaseRequest getSearch(SearchQuery<E> query);

    /**
     * Returns a list of elements
     * @param result The HTTP request's results
     * @param query The UniQuery itself
     * @return A list of elements
     */
    List<E> parseResults(HttpResponse<JsonNode> result, PredicateQuery query);

    /**
     * Returns an insert request
     * @param element The element to insert
     * @return An insert request
     * @throws NoSuchElementException
     */
    BaseRequest addElement(E element) throws NoSuchElementException;

    /**
     * Returns a delete request
     * @param element The element to delete
     * @return A delete request
     */
    BaseRequest delete(E element);
}
