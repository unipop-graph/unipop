package org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public abstract class ElasticFlatMapStep<S extends Element, E extends Element > extends AbstractStep<S,E> {
    protected final ArrayList<HasContainer> hasContainers;
    protected final Direction direction;
    protected final ElasticService elasticService;
    protected final Integer resultsLimit;

    private Iterator<ElasticTraverser> traversers;
    private ElasticTraverser currentTraverser;

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
        while(currentTraverser == null || !currentTraverser.hasNext()) {
            if(traversers == null || !traversers.hasNext())
                loadData();
            currentTraverser = traversers.next();
        }
        return currentTraverser.next();
    }

    private void loadData() {
        LinkedList<ElasticTraverser> traversers = new LinkedList<>();
        do{
            Traverser.Admin<S> traverser = this.starts.next();
            ElasticTraverser elasticTraverser = new ElasticTraverser(traverser, this);
            traversers.add(elasticTraverser);
        }while(this.starts.hasNext());
        this.traversers = traversers.iterator();
        load(traversers);
    }

    protected abstract void load(List<ElasticTraverser> iterator);

    @Override
    public void reset() {
        super.reset();
        traversers = null;
        currentTraverser = null;
    }

    public class ElasticTraverser implements Iterator<Traverser<E>> {
        private Traverser.Admin<S> traverer;
        private AbstractStep<S, E> step;
        private List<E> results = new ArrayList<>();
        private Iterator<E> iterator = null;

        private ElasticTraverser(Traverser.Admin<S> traverer, AbstractStep<S, E> step) {
            this.traverer = traverer;
            this.step = step;
        }

        @Override
        public boolean hasNext() {
            if(iterator == null) iterator = results.iterator();
            return iterator.hasNext();
        }

        @Override
        public Traverser<E> next() { return traverer.split(iterator.next(), step); }

        public S getElement() { return traverer.get();}
        public List<E> getResults() { return results;}
        public void addResult(E result) { results.add(result);}
        public void clearResults() { results.clear(); }
    }
}
