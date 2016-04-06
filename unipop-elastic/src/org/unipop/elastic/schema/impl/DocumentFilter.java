package org.unipop.elastic.schema.impl;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.elasticsearch.index.query.FilterBuilder;
import org.javatuples.Pair;
import org.unipop.elastic.schema.Filter;

import java.util.Iterator;
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

    public void add(Iterator<Pair<String, Object>> fields, P<?> predicate) {

    }
}
