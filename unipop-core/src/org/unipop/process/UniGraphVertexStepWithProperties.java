package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Predicates;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by sbarzilay on 4/9/16.
 */
public class UniGraphVertexStepWithProperties<E extends Element> extends UniGraphVertexStep<E> {
    public UniGraphVertexStepWithProperties(VertexStep vertexStep, Predicates predicates, ControllerManager controllerManager) {
        super(vertexStep, predicates, controllerManager);
    }

    public UniGraphVertexStepWithProperties(UniGraphVertexStep uniGraphVertexStep){
        super(uniGraphVertexStep.getDirection(), uniGraphVertexStep.getReturnClass(),
                uniGraphVertexStep.getPredicates(), uniGraphVertexStep.getLabels(),
                uniGraphVertexStep.getEdgeLabels(), uniGraphVertexStep.controllerManager, uniGraphVertexStep.getTraversal());
    }

    @Override
    protected Iterator<BaseEdge> getEdges(List<BaseVertex> vertices) {
        Iterable<BaseEdge> edges = () -> super.getEdges(vertices);
        controllerManager.properties(StreamSupport.stream(edges.spliterator(),false)
                .flatMap(edge -> Stream.of(edge.inVertex(), edge.outVertex())).map(vertex -> ((BaseElement) vertex))
                .collect(Collectors.toList()));
        return edges.iterator();
    }
}
