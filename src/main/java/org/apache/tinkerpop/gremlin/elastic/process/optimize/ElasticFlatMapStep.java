package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;

public abstract class ElasticFlatMapStep<S extends Element, E extends Element > extends FlatMapStep<S,E> {
    protected final ArrayList<HasContainer> hasContainers;
    protected final Direction direction;
    protected final ElasticService elasticService;
    protected final Integer resultsLimit;
    private Map<Traverser.Admin<S>, ArrayList<E>> results;


    public ElasticFlatMapStep(Traversal.Admin traversal, Optional<String> label, ElasticService elasticService, ArrayList<HasContainer> hasContainers, Direction direction,Integer resultsLimit) {
        super(traversal);
        if(label.isPresent()) setLabel(label.get());
        this.elasticService = elasticService;
        this.hasContainers = hasContainers;
        this.direction = direction;
        this.resultsLimit = resultsLimit;
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (results == null) {
            if (!starts.hasNext()) throw FastNoSuchElementException.instance();
            List<Traverser.Admin<S>> traversers = new ArrayList<>();
            starts.forEachRemaining(traversers::add);
            starts.add(traversers.iterator());
            results = query(traversers);
        }
        return super.processNextStart();
    }

    protected abstract Map<Traverser.Admin<S>, ArrayList<E>> query(List<Traverser.Admin<S>> traversalIterator);

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        ArrayList<E> returnValue = results.get(traverser);
        if(returnValue != null)
            return returnValue.iterator();
        return EmptyIterator.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.results = null;
    }

    protected void putOrAddToList(Map map, Object key, Object value) {
        Object list = map.get(key);
        if(list == null || !(list instanceof List)) {
            list = new ArrayList();
            map.put(key, list);
        }
        ((List)list).add(value);
    }
}
