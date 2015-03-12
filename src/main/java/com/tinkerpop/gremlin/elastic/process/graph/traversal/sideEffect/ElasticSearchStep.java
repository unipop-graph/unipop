package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.process.graph.util.HasContainer;

import java.util.List;

/**
 * Created by Eliran on 11/3/2015.
 */
public interface ElasticSearchStep {
    Object[] getIds();
    void addIds(Object[] ids);
    void setLabel(String label);
    void addPredicates(List<HasContainer> containerList);
    List<HasContainer> getPredicates();
    void addId(Object id);
    void clearIds();
    void clearPredicates();

}
