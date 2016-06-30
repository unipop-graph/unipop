package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.process.properties.PropertyFetcher;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

public abstract class UniPredicatesStep<S, E> extends UniBulkStep<S, E> implements PropertyFetcher {

    protected Set<String> propertyKeys;

    public UniPredicatesStep(Traversal.Admin traversal, UniGraph graph) {
        super(traversal, graph);
        this.propertyKeys = new HashSet<>();
    }

    @Override
    public void addPropertyKey(String key) {
        if (propertyKeys != null)
            propertyKeys.add(key);
    }

    @Override
    public void fetchAllKeys() {
        this.propertyKeys = null;
    }

    @Override
    public Set<String> getKeys() {
        return propertyKeys;
    }
}
