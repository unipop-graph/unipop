package org.unipop.process.count;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.provider.ControllerProvider;
import org.unipop.controller.Predicates;

import java.util.*;

/**
 * Created by Gilad on 02/11/2015.
 */
public class UniGraphCountStep<E extends Element> extends ReducingBarrierStep<E, Long> {
    //region Constructor
    public UniGraphCountStep(Traversal.Admin traversal, Class elementClass, Predicates predicates, Object[] ids, String[] edgeLabels, Optional<Direction> direction, ControllerProvider controllerProvider) {
        super(traversal);

        this.controllerProvider = controllerProvider;
        this.direction = direction;
        this.elementClass = elementClass;
        this.bulk = new ArrayList<>();

        this.setSeedSupplier(() -> {
            if (!this.previousStep.equals(EmptyStep.instance())) {
                return 0L;
            }

            if (Vertex.class.isAssignableFrom(elementClass)) {
                return this.controllerProvider.vertexCount(predicates);
            } else {
                return this.controllerProvider.edgeCount(predicates);
            }

        });


        this.setBiFunction((seed, traverser) -> {
            E element = traverser.get();
            bulk.add(element);


            Long bulkElementCount = 0L;
            //TODO: configure bulk size dynamically
            if (bulk.size() > 100 || !this.starts.hasNext()) {
                bulkElementCount = this.controllerProvider.edgeCount(bulk.stream().map(e -> (Vertex)e).toArray(size -> new Vertex[size]), direction.get(), edgeLabels, predicates);
                bulk.clear();
            }

            return seed + bulkElementCount;
        });
    }
    //endregion

    //region Fields
    private ControllerProvider controllerProvider;
    private Optional<Direction> direction;
    private Class elementClass;
    private Collection<E> bulk;
    //endregion
}
