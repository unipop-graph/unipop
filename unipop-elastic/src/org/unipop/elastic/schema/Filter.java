package org.unipop.elastic.schema;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;

import java.util.Set;

public interface Filter {
    boolean merge(Filter newFilter);
    FilterBuilder getFilterBuilder();
    Set<String> getIndices();
}
