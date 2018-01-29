package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.*;
import org.unipop.jdbc.utils.ContextManager;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A schema that represents an element in a JDBC data source
 * @param <E> Element
 */
public interface JdbcSchema<E extends Element> extends ElementSchema<E> {

    /**
     * Converts a SearchQuery to a select statement
     * @param query A search query
     * @param predicates A predicates holder
     * @return A select statement
     */
    Select getSearch(SearchQuery<E> query, PredicatesHolder predicates);

    /**
     * Returns a list of elements
     * @param result The query results
     * @param query The UniQuery
     * @return A list of elements
     */
    Select createSelect(SearchQuery<E> query, PredicatesHolder predicatesHolder, Field... fields);
    Select getSearch(SearchQuery<E> query, PredicatesHolder predicates, Field... fields);
    List<E> parseResults(List<Map<String, Object>> result, PredicateQuery query);

    /**
     * @return the full table name, including database prefix
     */
    String getTable();

    /**
     * @param element The element
     * @return The element's Id
     */
    default Object getId(E element) {
        return element.id();
    }

    /**
     * Converts an element to a JDBC row
     * @param element An element
     * @return A JDBC row
     */
    default Row toRow(E element) {
        Map<String, Object> fields = this.toFields(element);
        if (fields == null) {
            return null;
        }

        String table = this.getTable();
        Object id = this.getId(element);

        return new JdbcSchema.Row(table, id, fields);
    }

    /**
     * @param element An element
     * @return An insert statement
     */
    Query getInsertStatement(E element);

    class Row {
        private final String table;
        private final Object id;
        private final Map<String, Object> fields;

        private final Set<String> idField;


        public Row(String table, Object id, Map<String, Object> fields) {
            this.table = table;
            this.id = id;
            this.fields = fields;

            this.idField = this.fields.entrySet().stream().filter(en -> id.equals(en.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
        }

        public String getTable() {
            return table;
        }

        public Object getId() {
            return id;
        }

        public Map<String, Object> getFields() {
            return fields;
        }

        public Set<String> getIdField() {
            return this.idField;
        }

        @Override
        public String toString() {
            return "Row{" +
                    "fields=" + fields +
                    ", table='" + table + '\'' +
                    ", id=" + id +
                    ", idField='" + idField + '\'' +
                    '}';
        }
    }
}
