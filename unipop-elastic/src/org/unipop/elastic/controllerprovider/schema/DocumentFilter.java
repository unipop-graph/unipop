package org.unipop.elastic.controllerprovider.schema;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.elasticsearch.index.query.FilterBuilder;
import org.unipop.elastic.schema.Filter;

import java.util.Map;
import java.util.Set;

public class DocumentFilter implements Filter {
    private String index;

    public DocumentFilter(String index) {
        this.index = index;
    }

    @Override
    public boolean merge(Filter newFilter) {
        return false;
    }

    @Override
    public FilterBuilder getFilterBuilder() {
        return null;
    }

    @Override
    public Set<String> getIndices() {
        return null;
    }

    public void add(Map<String, Object> fields, P<?> predicate) {

    }

    public void add(String key, Object value, P<?> predicate) {

    }
}
