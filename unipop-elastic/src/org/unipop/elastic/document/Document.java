package org.unipop.elastic.document;

import java.util.Map;

public class Document {
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
