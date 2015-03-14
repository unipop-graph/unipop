package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.*;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.*;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.FilterBuilder;

import java.util.*;
import java.util.function.Function;

/**
 * Created by Eliran on 11/3/2015.
 */
public  class ElasticSearchFlatMap<S extends  Element, E extends Element > extends AbstractStep<S,E> implements Reversible,ElasticSearchStep {
    private List<HasContainer> hasContainers;
    private List<Object> ids;
    private Function<Iterator<S>, Iterator<E>> function = null;
    private Traverser.Admin<S> head = null;
    private Iterator<E> iterator = Collections.emptyIterator();
    protected ElasticService elasticService;
    Iterator< Traverser.Admin<S>> formerStepIterator;
    protected List<Integer> jumpingPoints;

    public ElasticSearchFlatMap(Traversal traversal,ElasticService elasticService) {
        super(traversal);
        this.elasticService = elasticService;
        this.hasContainers = new ArrayList<>();
        this.ids = new ArrayList<Object>();
        this.jumpingPoints = new ArrayList<>();
    }

    @Override
    public void setTypeLabel(String label){
        this.setLabel(label);
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
    public void clearIds() {
        this.ids = new ArrayList<Object>();
    }
    @Override
    public void clearPredicates() {
        this.hasContainers = new ArrayList<HasContainer>();
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

    private int counter =0;
    @Override
    protected Traverser<E> processNextStart() {
        while (true) {

            if (this.iterator.hasNext()) {
                if(jumpingPoints.size() > 0 ){
                    while(jumpingPoints.size() > 0 && counter == jumpingPoints.get(0)){
                        this.head = formerStepIterator.next();
                        jumpingPoints.remove(0);
                    }
                }
                counter++;
                final Traverser<E> end = this.head.split(this.iterator.next(), this);
                return end;
            } else {
                List<Traverser.Admin<S>> steps = new ArrayList<Traverser.Admin<S>>();
                Traverser.Admin<S> first = this.starts.next();
                Traverser.Admin<S> last = first;
                steps.add(first);
                List<S> elementsContainer = new ArrayList<S>();
                elementsContainer.add(last.get());
                while(this.starts.hasNext()){
                    last = this.starts.next();
                    elementsContainer.add(last.get());
                    steps.add(last);
                }
                formerStepIterator = steps.iterator();
                this.head = formerStepIterator.next();
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

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this,this.hasContainers);
    }

}
