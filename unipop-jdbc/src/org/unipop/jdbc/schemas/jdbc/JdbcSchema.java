package org.unipop.jdbc.schemas.jdbc;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.schema.element.ElementSchema;

import java.util.List;
import java.util.Map;

/**
 * @author Gur Ronen
 * @since 6/12/2016
 */
public interface JdbcSchema<E extends Element> extends ElementSchema<E> {

    List<E> parseResults(String result, PredicateQuery query);

    /**
     * @return the full table name, including database prefix
     */
    String getTable();

    default Object getId(E element) {
        return element.id();
    }

    default Row toRow(E element) {
        Map<String, Object> fields = this.toFields(element);
        if (fields == null) {
            return null;
        }

        String table = this.getTable();
        Object id = this.getId(element);

        return new JdbcSchema.Row(table, id, fields);
    }

    class Row {
        private final String table;
        private final Object id;
        private final Map<String, Object> fields;

        private final String idField;


        public Row(String table, Object id, Map<String, Object> fields) {
            this.table = table;
            this.id = id;
            this.fields = fields;

            this.idField = this.fields.entrySet().stream().filter(en -> id.equals(en.getValue())).map(Map.Entry::getKey).findFirst().get();
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

        public String getIdField() {
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
