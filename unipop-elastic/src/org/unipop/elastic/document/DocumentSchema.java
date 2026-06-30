package org.unipop.elastic.document;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;
import java.util.Map;

/**
 * A schema that is represented by a document in ES.
 * @param <E> Element
 */
public interface DocumentSchema<E extends Element> extends ElementSchema<E> {

    /**
     * Returns the index property schema of the document schema
     * @return Index property schema
     */
    IndexPropertySchema getIndex();

    /**
     * Converts a Search query to an ES 8 Query
     * @param query A search query
     * @return A Query
     */
    Query getSearch(SearchQuery<E> query);

    /**
     * Return a list of elements from ES 8 search hits
     * @param hits  The hits returned by the ES query
     * @param query The UniQuery itself
     * @return A list of elements
     */
    List<E> parseResults(List<Hit<Map>> hits, PredicateQuery query);

    /**
     * Returns a BulkOperation to insert or update a document
     * @param element The element to add or update
     * @param create  Whether to create or update
     * @return A BulkOperation
     */
    BulkOperation addElement(E element, boolean create);

    /**
     * Returns a BulkOperation to delete a document from ES
     * @param element The element to delete
     * @return A BulkOperation
     */
    BulkOperation delete(E element);
}
