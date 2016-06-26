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
        String type = ObjectUtils.firstNonNull(fields.remove("_type"), fields.remove("type"), this.getType(), element.label()).toString();
        if (!checkType(type)) return null;
        String id = ObjectUtils.firstNonNull(fields.remove("_id"), fields.remove("id"), element.id()).toString();
        if(id == null) return null;
        String index = getIndex();
        if(index == null) return null;
        return new Document(index, type, id, fields);
    }

    default E fromDocument(Document document){
        if(!checkType(document.getType())) return null;
        if(!checkIndex(document.getIndex())) return null;
        Map<String, Object> fields = document.getFields();
        fields.put("_id", document.getId());
        fields.put("_type", document.getType());
        return this.fromFields(fields);
    }

    default boolean checkIndex(String index) {
        if(index == null) return false;
        if(getIndex() != null && !getIndex().equals("*") && !getIndex().equals(index)) return false;
        return true;
    }

    default boolean checkType(String type) {
        if(type == null) return false;
        if(getType() != null && !getType().equals("*") && !getType().equals(type)) return false;
        return true;
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
