package org.unipop.elastic.document.schema;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.schema.ElementSchema;

import java.util.Map;

public interface DocSchema<E extends Element> extends ElementSchema<E> {

    String getIndex();
    String getType();

    default Document toDocument(E element) {
        Map<String, Object> fields = this.toFields(element);
        if(fields == null) return null;
        Object type = ObjectUtils.firstNonNull(fields.remove("_type"), fields.remove("type"), this.getType(), element.label());
        String id = ObjectUtils.firstNonNull(fields.remove("_id"), fields.remove("id"), element.id()).toString();
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
