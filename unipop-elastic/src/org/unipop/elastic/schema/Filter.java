package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.elasticsearch.index.query.FilterBuilder;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.Set;

public interface Filter {
    boolean merge(Filter newFilter);
    FilterBuilder getFilterBuilder();
    Set<String> getIndices();
    void add(Iterator<Pair<String, Object>> fields, P<?> predicate);
}
