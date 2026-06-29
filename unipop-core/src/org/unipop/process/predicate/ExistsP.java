package org.unipop.process.predicate;


/**
 * Created by Roman on 11/9/2015.
 */
public class ExistsP<V> extends org.apache.tinkerpop.gremlin.process.traversal.P<V> {
    //region Constructor
    public ExistsP() {
        // P now overloads P(PBiPredicate, V) and P(PBiPredicate, GValue<V>); the (V) cast
        // disambiguates super(null, null) to the value constructor.
        super(null, (V) null);
    }
    //endregion
}
