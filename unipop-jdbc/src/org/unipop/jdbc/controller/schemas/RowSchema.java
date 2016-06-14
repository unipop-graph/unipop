package org.unipop.jdbc.controller.schemas;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.unipop.common.schema.ElementSchema;

import java.util.Map;
import java.util.Set;

/**
 * Created by GurRo on 6/12/2016.
 */
public interface RowSchema<E extends Element> extends ElementSchema<E> {
    String getDatabase();

    default String getTable(E element) {
        return element.label();
    }

    default Object getId(E element) {
        return element.id();
    }

    default Row toRow(E element) {
        Map<String, Object> fields = this.toFields(element);
        if (fields == null) {
            return null;
        }

        String database = this.getDatabase();
        String table = this.getTable(element);
        Object id = this.getId(element);

        return new RowSchema.Row(database, table, id, fields);
    }

    class Row {
        private final String database;
        private final String table;
        private final Object id;
        private final Map<String, Object> fields;


        public Row(String database, String table, Object id, Map<String, Object> fields) {
            this.database = database;
            this.table = table;
            this.id = id;
            this.fields = fields;
        }

        public String getDatabase() {
            return database;
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
    }
}
