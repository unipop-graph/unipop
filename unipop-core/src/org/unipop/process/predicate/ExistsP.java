package org.unipop.process.predicate;


/**
 * Created by Roman on 11/9/2015.
 */
public class ExistsP<V> extends org.apache.tinkerpop.gremlin.process.traversal.P<V> {
    //region Constructor
    public ExistsP() {
        super((org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate<V, V>) null, (V) null);
    }
    //endregion
}
