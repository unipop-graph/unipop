//package org.unipop.process.count;
//
//import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
//import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
//import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
//import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
//import org.apache.tinkerpop.gremlin.structure.Direction;
//import org.apache.tinkerpop.gremlin.structure.Element;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.unipop.query.UniQuery;
//import org.unipop.query.controller.ControllerManager;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * Created by Gilad on 02/11/2015.
// */
//public class UniGraphCountStep<E extends Element> extends ReducingBarrierStep<E, Long> {
//    //region Constructor
//    public UniGraphCountStep(Traversal.Admin traversal, Class elementClass, List<HasContainer> hasContainers, Object[] ids, String[] edgeLabels, Optional<Direction> direction, ControllerManager controllerManager) {
//        super(traversal);
//        this.hasContainers = hasContainers;
//        this.controllerManager = controllerManager;
//        this.direction = direction.get();
//        this.elementClass = elementClass;
//        this.bulk = new ArrayList<>();
//
//        List<CountController> countControllers = controllerManager.getControllers(CountController.class);
//        List<EdgeCountController> edgeCountControllers = controllerManager.getControllers(EdgeCountController.class);
//        UniQuery uniQuery = new UniQuery(null, edgeLabels,ids, hasContainers, 0);
//
//        this.setSeedSupplier(() -> {
//            if (!this.previousStep.equals(EmptyStep.instance())) {
//                return 0L;
//            }
//            return countControllers.stream().collect(Collectors.summingLong(controller -> controller.count(uniQuery)));
//        });
//
//
//        this.setBiFunction((seed, traverser) -> {
//            E element = traverser.get();
//            bulk.add(element);
//
//
//            Long bulkElementCount = 0L;
//            //TODO: configure bulk size dynamically
//            if (bulk.size() > 100 || !this.starts.hasNext()) {
//                Vertex[] vertices = bulk.toArray(new Vertex[0]);
//                bulkElementCount = edgeCountControllers.stream().collect(Collectors.summingLong(edgeController ->
//                        edgeController.count(vertices, this.direction, edgeLabels, uniQuery)));
//                bulk.clear();
//            }
//
//            return seed + bulkElementCount;
//        });
//    }
//    //endregion
//
//    private final List<HasContainer> hasContainers;
//    //region Fields
//    private ControllerManager controllerManager;
//    private Direction direction;
//    private Class elementClass;
//    private Collection<E> bulk;
//    //endregion
//}
