package org.unipop.schema.property.type;


import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.unipop.schema.element.ElementSchema;

/**
 * Created by sbarzilay on 8/18/16.
 */

/**
 * A property type
 */
public interface PropertyType {
    /**
     * Returns the type's name
     * @return Type's name
     */
    String getType();

    /**
     * Translates the predicate to match the property type
     * @param predicate A predicate
     * @param <V>
     * @return Translated predicate
     */
    default <V> P<V> translate(P<V> predicate){
        return predicate;
    }
}
