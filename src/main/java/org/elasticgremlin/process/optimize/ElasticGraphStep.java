package org.elasticgremlin.process.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticservice.*;

import java.util.Iterator;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {

    private final Predicates predicates;
    private final ElasticService elasticService;

    public ElasticGraphStep(GraphStep originalStep, Predicates predicates, ElasticService elasticService) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getIds());
        originalStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(label -> this.addLabel(label.toString()));
        this.predicates = predicates;
        this.elasticService = elasticService;
        Object[] ids = super.getIds();
        if(ids.length > 0) {
            predicates.hasContainers.add(new HasContainer("~id", P.within(ids)));
        }
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Vertex> vertices() {

        return elasticService.searchVertices(predicates);
    }

    private Iterator<? extends Edge> edges() {
         return elasticService.searchEdges(predicates, null, null);
    }
}
