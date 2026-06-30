package org.unipop.elastic.document;

import java.util.Map;

public class Document {
    private final String index;
    private final String id;
    private final Map<String, Object> fields;

    public Document(String index, String id, Map<String, Object> fields) {
        this.index = index;
        this.id = id;
        this.fields = fields;
    }

    public String getIndex() { return index; }
    public String getId() { return id; }
    public Map<String, Object> getFields() { return fields; }

    @Override
    public String toString() {
        return "Document{index='" + index + "', id='" + id + "', fields=" + fields + '}';
    }
}
