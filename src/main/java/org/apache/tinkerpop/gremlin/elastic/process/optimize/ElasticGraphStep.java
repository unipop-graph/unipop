package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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
}
