package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Eliran on 11/3/2015.
 */
public class ElasticSearchFlatMap<S extends Iterator<? extends Element>, E extends Element > extends FlatMapStep<S,E> implements Reversible,ElasticSearchStep {
    private List<HasContainer> hasContainers;
    private List<Object> ids;
    public ElasticSearchFlatMap(Traversal traversal) {
        super(traversal);
        this.hasContainers = new ArrayList<>();
        this.ids = new ArrayList<Object>();
    }

    @Override
    public Object[] getIds() {
        return this.ids.toArray();
    }

    @Override
    public void addIds(Object[] ids) {
        this.ids.addAll(Arrays.asList(ids));
    }
    @Override
    public void addId(Object id) {
        this.ids.add(id);
    }

    @Override
    public void addPredicates(List<HasContainer> containerList) {
        this.hasContainers.addAll(containerList);
    }

    @Override
    public List<HasContainer> getPredicates() {
        return this.hasContainers;
    }


}
