package org.apache.tinkerpop.gremlin.elastic.process.graph.traversal.steps;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_P_PA_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {

    private final ArrayList<HasContainer> hasContainers;
    private final ElasticService elasticService;
    private Integer resultLimit;

    public ElasticGraphStep(GraphStep originalStep, ArrayList<HasContainer> hasContainers, ElasticService elasticService,Integer resultLimit) {
        super(originalStep.getTraversal(),originalStep.getReturnClass(),originalStep.getIds());
        if (originalStep.getLabel().isPresent()) this.setLabel(originalStep.getLabel().get().toString());
        this.hasContainers = hasContainers;
        this.elasticService = elasticService;
        this.resultLimit = resultLimit;
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Vertex> vertices() {
        ArrayList<HasContainer> hasList = hasContainers;
        Object[] ids = super.getIds();
        if(ids.length > 0) {
            hasList  = (ArrayList<HasContainer>) hasContainers.clone();
            hasList.add(new HasContainer("~id", Contains.within, ids));
        }
        return elasticService.searchVertices(hasList, resultLimit);
    }

    private Iterator<? extends Edge> edges() {
        ArrayList<HasContainer> hasList = hasContainers;
        Object[] ids = super.getIds();
        if(ids.length > 0) {
            hasList  = (ArrayList<HasContainer>) hasContainers.clone();
            hasList.add(new HasContainer("~id", Contains.within, ids));
        }
         return elasticService.searchEdges(hasList, resultLimit, null, null);
    }


    public void generateTraversers(final TraverserGenerator traverserGenerator) {

        try {
            this.start = this.iteratorSupplier.get();
            if (null != this.start) {

                if (this.start instanceof Iterator) {
                    List<E> newListForIterator = new ArrayList<>();
                    Iterator<E> iter = (Iterator<E>) this.start;
                    while(iter.hasNext()){
                        E next = iter.next();

                        this.starts.add(B_O_P_PA_S_SE_SL_TraverserGenerator.instance().generate(next,this,1l));
                        newListForIterator.add(next);
                    }
                    this.start = newListForIterator.iterator();

                } else {
                    this.starts.add(traverserGenerator.generate((E) this.start, this, 1l));
                }
            }
        }catch (NoSuchElementException ex){
            throw ex;
        }
        catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (this.first) {
            generateTraversers(this.getTraversal().getTraverserGenerator());
            this.first = false;
        }
        return this.starts.next();
    }

}
