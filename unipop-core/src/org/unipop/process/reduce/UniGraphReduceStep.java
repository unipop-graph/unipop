package org.unipop.process.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.process.properties.PropertyFetcher;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.query.aggregation.reduce.ReduceVertexQuery;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 6/27/2016
 * <p>
 * This step is the replacement for reducing functions in gremlin to Unipop implementations
 * {@link org.unipop.query.aggregation.reduce.ReduceQuery.Op} for a list of reduce methods.
 */
public class UniGraphReduceStep<E extends Element> extends ReducingBarrierStep<E, Number> implements ReceivesPredicatesHolder<E, E>, PropertyFetcher {

    private final PredicatesHolder predicatesHolder;
    private ControllerManager controllerManager;
    private Direction direction;
    private Class elementClass;
    private Collection<E> bulk;


    //region Constructor
    public UniGraphReduceStep(Traversal.Admin traversal, Class elementClass, PredicatesHolder predicatesHolder, ControllerManager controllerManager, ReduceQuery.Op op) {
        super(traversal);
        this.predicatesHolder = predicatesHolder;
        this.controllerManager = controllerManager;
        this.elementClass = elementClass;
        this.bulk = new ArrayList<>();

        List<ReduceQuery.ReduceController> reduceControllers = controllerManager.getControllers(ReduceQuery.ReduceController.class);
        List<ReduceVertexQuery.ReduceVertexController> reduceVertexControllers = controllerManager.getControllers(ReduceVertexQuery.ReduceVertexController.class);

        UniQuery uniQuery = new ReduceQuery(this.predicatesHolder, new StepDescriptor(this), op, );

        this.setSeedSupplier(() -> {
            if (!this.previousStep.equals(EmptyStep.instance())) {
                return 0L;
            }
            return reduceControllers.stream().collect(Collectors.summingLong(controller -> controller.count(uniQuery)));
        });


        this.setReducingBiOperator((seed, traverser) -> {
            E element = traverser.get();
            bulk.add(element);


            Long bulkElementCount = 0L;
            //TODO: configure bulk size dynamically
            if (bulk.size() > 100 || !this.starts.hasNext()) {
                Vertex[] vertices = bulk.toArray(new Vertex[0]);
                bulkElementCount = edgeCountControllers.stream().collect(Collectors.summingLong(edgeController ->
                        edgeController.count(vertices, this.direction, edgeLabels, uniQuery)));
                bulk.clear();
            }

            return seed + bulkElementCount;
        });
    }

    }


    @Override
    public Long projectTraverser(final Traverser.Admin<E> traverser) {
        return traverser.bulk();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.BULK);
    }

}
