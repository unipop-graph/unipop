package org.unipop.common.property;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Map;

public interface PropertySchema {
    Map<String, Object> toProperties(Map<String, Object> source) ;
    Map<String, Object> toFields(Map<String, Object> properties);
    PredicatesHolder toPredicates(HasContainer has);
}
