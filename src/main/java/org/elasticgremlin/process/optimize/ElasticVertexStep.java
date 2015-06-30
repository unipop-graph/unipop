package org.elasticgremlin.process.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.structure.BaseVertex;

import java.util.Iterator;

public class ElasticVertexStep<E extends Element> extends VertexStep<E> {
    private final Predicates predicates;

    public ElasticVertexStep(VertexStep originalStep, Predicates predicates) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getDirection(),
                originalStep.getEdgeLabels());
        originalStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(this::addLabel);
        this.predicates = predicates;
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        Vertex vertex = traverser.get();
        if (!(BaseVertex.class.isAssignableFrom(vertex.getClass()))) return super.flatMap(traverser);
        BaseVertex baseVertex = (BaseVertex) vertex;

        if (Vertex.class.isAssignableFrom(this.getReturnClass()))
            return (Iterator<E>) baseVertex.vertices(this.getDirection(), this.getEdgeLabels(), predicates);

        return (Iterator<E>) baseVertex.edges(this.getDirection(), this.getEdgeLabels(), predicates);
    }
}
