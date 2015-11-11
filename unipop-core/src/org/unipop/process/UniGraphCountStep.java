package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;
import org.unipop.controllerprovider.ControllerManager;

import java.util.*;

/**
 * Created by Gilad on 02/11/2015.
 */
public class UniGraphCountStep<E extends Element> extends ReducingBarrierStep<E, Long> {
    //region Constructor
    public UniGraphCountStep(Traversal.Admin traversal, Class elementClass, Predicates predicates, Object[] ids, String[] edgeLabels, Optional<Direction> direction, ControllerManager controllerManager) {
        super(traversal);

        this.controllerManager = controllerManager;
        this.direction = direction;
        this.elementClass = elementClass;
        this.bulk = new ArrayList<>();

        this.setSeedSupplier(() -> {
            if (!this.previousStep.equals(EmptyStep.instance())) {
                return 0L;
            }

            if (Vertex.class.isAssignableFrom(elementClass)) {
                return this.controllerManager.vertexCount(predicates);
            } else {
                return this.controllerManager.edgeCount(predicates);
            }

        });


        this.setBiFunction((seed, traverser) -> {
            E element = traverser.get();
            bulk.add(element);


            Long bulkElementCount = 0L;
            //TODO: configure bulk size dynamically
            if (bulk.size() > 1000000 || !this.starts.hasNext()) {
                bulkElementCount = this.controllerManager.edgeCount((Vertex[])bulk.toArray(), direction.get(), edgeLabels, predicates);
                if (direction.isPresent() &&
                        direction.get().equals(Direction.BOTH) &&
                        Vertex.class.isAssignableFrom(elementClass)) {
                    bulkElementCount *= 2;
                }
                bulk.clear();
            }

            return seed + bulkElementCount;
        });
    }
    //endregion

    //region Fields
    private ControllerManager controllerManager;
    private Optional<Direction> direction;
    private Class elementClass;
    private Collection<E> bulk;
    //endregion
}
