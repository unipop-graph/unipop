package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;


import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.function.Function;

/**
 * Created by Eliran on 11/3/2015.
 */
public class ElasticSearchFlatMap<S extends  Element, E extends Element > extends AbstractStep<S,E> implements Reversible,ElasticSearchStep {
    private List<HasContainer> hasContainers;
    private List<Object> ids;
    private Function<Iterator<S>, Iterator<E>> function = null;
    private Traverser.Admin<S> head = null;
    private Iterator<E> iterator = Collections.emptyIterator();

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


    public void setFunction(final Function<Iterator<S>, Iterator<E>> function) {
        this.function = function;
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {

                final Traverser<E> end = this.head.split(this.iterator.next(), this);
                return end;
            } else {
                Traverser.Admin<S> last = this.starts.next();
                List<S> elementsContainer = new ArrayList<>();
                elementsContainer.add(last.get());
                while(this.starts.hasNext()){
                    last = this.starts.next();
                    elementsContainer.add(last.get());
                }
                this.head = last;
                if (PROFILING_ENABLED) TraversalMetrics.start(this);
                this.iterator = this.function.apply(elementsContainer.iterator());
                if (PROFILING_ENABLED) TraversalMetrics.stop(this);
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.iterator = Collections.emptyIterator();
    }

}
