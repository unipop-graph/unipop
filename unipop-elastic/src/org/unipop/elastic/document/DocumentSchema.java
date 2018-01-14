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

/**
 * A schema that is represented by a document in ES.
 * @param <E> Element
 */
public interface DocumentSchema<E extends Element> extends ElementSchema<E>{

    /**
     * Returns the index property schema of the document schema
     * @return Index property schema
     */
    IndexPropertySchema getIndex();

    /**
     * Converts a Search query to a Query builder
     * @param query A search query
     * @return A query builder
     */
    QueryBuilder getSearch(SearchQuery<E> query);

    /**
     * Return a list of elements
     * @param result The result of the ES query
     * @param query The UniQuery itself
     * @return A list of elements
     */
    List<E> parseResults(String result, PredicateQuery query);

    /**
     * Returns an action to insert or update a document
     * @param element The element to add or update
     * @param create Whether to create or update
     * @return An action
     */
    BulkableAction<DocumentResult> addElement(E element, boolean create);

    /**
     * Deletes a document from ES
     * @param element The element to delete
     * @return A delete builder
     */
    Delete.Builder delete(E element);
}
