package org.unipop.process.group.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

/**
 * Created by Gilad on 02/11/2015.
 */
public class SemanticValuesTraversal implements Traversal {
    public enum Type {
        property
    }

    //region Constructor
    public SemanticValuesTraversal(SemanticValuesTraversal.Type type, String key) {
        this.type = type;
        this.key = key;
    }
    //endregion

    //region Traversal Implementation
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object next() {
        throw FastNoSuchElementException.instance();
    }
    //endregion

    //region Properties
    public String getKey() {
        return this.key;
    }

    public SemanticValuesTraversal.Type getType() {
        return this.type;
    }
    //endregion

    //region Fields
    private String key;
    private SemanticValuesTraversal.Type type;
    //endregion
}
