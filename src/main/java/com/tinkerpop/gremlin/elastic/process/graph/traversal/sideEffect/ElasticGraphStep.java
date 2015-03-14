package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traverser.B_O_P_PA_S_SE_SL_TraverserGenerator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.index.query.*;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> implements ElasticSearchStep {

    ElasticService elasticService;

    List<Object> idsExtended;
    BoolFilterBuilder boolFilterBuilder;

    public  List<HasContainer> hasContainers = new ArrayList<HasContainer>();
    private String typelLabel;

    public ElasticGraphStep(final Traversal traversal, final ElasticGraph graph, final Class<E> returnClass, Optional<String> label, final Object... ids) {
        super(traversal, graph, returnClass, ids);
        this.idsExtended = new ArrayList<>();
        this.boolFilterBuilder = FilterBuilders.boolFilter();
        idsExtended.addAll(Arrays.asList(ids));

        if (label.isPresent()) this.setLabel(label.get());
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        this.elasticService = graph.elasticService;
    }

    private Iterator<? extends Vertex> vertices() {
        Iterator<Vertex> vertexIterator = elasticService.searchVertices(FilterBuilderProvider.getFilter(this),this.typelLabel);
        return vertexIterator;
    }

    private Iterator<? extends Edge> edges() {
        return elasticService.searchEdges(FilterBuilderProvider.getFilter(this),typelLabel);
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
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }

    @Override
    public void setTypeLabel(String label){
        this.typelLabel = label;
    }
    private String label() {
        return this.getLabel().isPresent() ? this.getLabel().get() : null;
    }

    @Override
    public void addIds(Object[] ids) {
        idsExtended.addAll(Arrays.asList(ids));
    }

    @Override
    public void addPredicates(List<HasContainer> containerList) {
        this.hasContainers.addAll(containerList);
    }

    @Override
    public List<HasContainer> getPredicates() {
        return this.hasContainers;
    }

    @Override
    public void addId(Object id) {
        this.idsExtended.add(id);
    }

    @Override
    public void clearIds() {
        this.idsExtended = new ArrayList<Object>();
    }
    @Override
    public void clearPredicates() {
        this.hasContainers = new ArrayList<HasContainer>();
    }


    @Override
    public String toString(){
        if(this.typelLabel!=null)
            return TraversalHelper.makeStepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.asList(this.typelLabel), this.getPredicates());
        return TraversalHelper.makeStepString(this, this.returnClass.getSimpleName().toLowerCase(), this.getPredicates());
    }

}
