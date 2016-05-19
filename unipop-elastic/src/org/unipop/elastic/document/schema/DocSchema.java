package org.unipop.elastic.document.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.common.schema.ElementSchema;

import java.util.Map;

public interface DocSchema<E extends Element> extends ElementSchema<E> {

    String getIndex();

    default String getType(E element) {
        return element.label();
    }

    default Object getId(E element) {
        return element.id();
    }

    default Document toDocument(E element) {
        Map<String, Object> fields = this.toFields(element);
        if(fields == null) return null;
        Object type = fields.getOrDefault("_type", fields.getOrDefault("type", getType(element)));
        String id = fields.getOrDefault("_id", fields.getOrDefault("id", getId(element))).toString();
        String index = getIndex();
        return new Document(index, type.toString(), id, fields);
    }

    class Document {
        private final String index;
        private final String type;
        private final String id;
        private final Map<String, Object> fields;

        public Document(String index, String type, String id, Map<String, Object> fields) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.fields = fields;
        }

        public String getIndex() {
            return index;
        }

        public String getType() {
            return type;
        }

        public String getId() {
            return id;
        }

        public Map<String, Object> getFields() {
            return fields;
        }
    }
}
