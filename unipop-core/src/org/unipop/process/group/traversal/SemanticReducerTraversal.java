package org.unipop.process.group.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

/**
 * Created by Gilad on 02/11/2015.
 */
public class SemanticReducerTraversal implements Traversal {
    public enum Type {
        count,
        min,
        max,
        cardinality
    }

    //region Constructor
    public SemanticReducerTraversal(SemanticReducerTraversal.Type type, String key) {
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

    public SemanticReducerTraversal.Type getType() {
        return this.type;
    }
    //endregion

    //region Fields
    private String key;
    private SemanticReducerTraversal.Type type;
    //endregion
}
