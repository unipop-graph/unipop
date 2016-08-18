package org.unipop.common.util;

import org.unipop.query.predicates.PredicatesHolder;

/**
 * @author GurRo
 * @since 6/13/2016
 *
 * Given an input of type {@link org.unipop.query.predicates.PredicatesHolder},
 * return T that represents the required resulting query format needed for the appropiate controller.
 *
 */
@FunctionalInterface
public interface PredicatesTranslator<T> {
    T translate(PredicatesHolder holder) ;

}
