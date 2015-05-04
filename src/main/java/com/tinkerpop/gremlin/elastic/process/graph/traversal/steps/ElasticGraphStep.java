package com.tinkerpop.gremlin.elastic.process.graph.traversal.steps;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traverser.B_O_P_PA_S_SE_SL_TraverserGenerator;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.*;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {

    private final ArrayList<HasContainer> hasContainers;
    private final ElasticService elasticService;
    private Integer resultLimit;

    public ElasticGraphStep(GraphStep originalStep, ArrayList<HasContainer> hasContainers, ElasticService elasticService,Integer resultLimit) {
        super(originalStep.getTraversal(), originalStep.getGraph(ElasticGraph.class),originalStep.getReturnClass(),originalStep.getIds());
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

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        try {
            this.start = this.iteratorSupplier.get();
            if (null != this.start) {

                if (this.start instanceof Iterator) {
                    List<E> newListForIterator = new ArrayList<>();
                    Iterator<E> iter = (Iterator<E>) this.start;
                    while(iter.hasNext()){
                        E next = iter.next();

                        //B_O_PA_S_SE_SL_NC_Traverser<E> eb_o_pa_s_se_sl_nc_traverser = new B_O_PA_S_SE_SL_NC_Traverser<>(next, this);
                        this.starts.add(B_O_P_PA_S_SE_SL_TraverserGenerator.instance().generate(next,this,1l));
                        newListForIterator.add(next);
                    }
                    this.start = newListForIterator.iterator();
                    //this.starts.add(traverserGenerator.generateIterator((Iterator<E>) this.start, this, 1l));
                } else {
                    this.starts.add(traverserGenerator.generate((E) this.start, this, 1l));
                }
            }
        }catch (NoSuchElementException ex){
            throw ex;
        }
        catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }
}
