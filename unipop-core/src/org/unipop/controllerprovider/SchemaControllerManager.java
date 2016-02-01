package org.unipop.controllerprovider;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by sbarzilay on 30/01/16.
 */
public abstract class SchemaControllerManager implements ControllerManager {

    protected Map<String, Set<VertexController>> vertexControllers;
    protected Map<String, Set<EdgeController>> edgeControllers;

    public SchemaControllerManager() {
        vertexControllers = new HashMap<>();
        edgeControllers = new HashMap<>();
    }

    @Override
    public void close() {
        vertexControllers.entrySet().forEach(entry -> entry.getValue().forEach(VertexController::close));
        edgeControllers.entrySet().forEach(entry -> entry.getValue().forEach(EdgeController::close));
    }

    protected <T> void addController(Map<String, Set<T>> controllers, T controller, String label) {
        if (controllers.containsKey(label))
            controllers.get(label).add(controller);
        else
            controllers.put(label, new HashSet<T>() {{
                add(controller);
            }});
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        Object labels = getLabel(predicates);
        if (labels == null)
            return edgeControllers.entrySet().stream()
                    .flatMap(edgeControllers1 -> edgeControllers1.getValue().stream().map(edgeController -> {
                                Predicates p = new Predicates(predicates);
                                p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeControllers1.getKey())));
                                p.labels.add(edgeControllers1.getKey());
                                return edgeController.edges(p);
                            }
                    ).collect(Collectors.toList()).stream()).flatMap(baseEdgeIterator -> {
                        Iterable<BaseEdge> iterable = () -> baseEdgeIterator;
                        return StreamSupport.stream(iterable.spliterator(), false);
                    }).collect(Collectors.toList()).iterator();
        else if (labels instanceof String)
            return edgeControllers.get(labels.toString()).stream()
                    .map(edgeController -> edgeController.edges(predicates))
                    .flatMap(baseEdgeIterator -> {
                        Iterable<BaseEdge> iterable = () -> baseEdgeIterator;
                        return StreamSupport.stream(iterable.spliterator(), false);
                    }).collect(Collectors.toList()).iterator();
        else if (labels instanceof Iterable)
            return StreamSupport.stream(((Iterable<String>) labels).spliterator(), false).flatMap(label ->
                    edgeControllers.get(label).stream()
                            .map(vertexController -> {
                                Predicates p = new Predicates(predicates);
                                for (HasContainer hasContainer : p.hasContainers) {
                                    if (hasContainer.getKey().equals(T.label.getAccessor())) {
                                        p.hasContainers.remove(hasContainer);
                                        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(label)));
                                        break;
                                    }
                                }
                                return vertexController.edges(p);
                            })
                            .collect(Collectors.toList()).stream()
            ).flatMap(baseEdgeIterator -> {
                Iterable<BaseEdge> iterable = () -> baseEdgeIterator;
                return StreamSupport.stream(iterable.spliterator(), false);
            }).collect(Collectors.toList()).iterator();

        return EmptyIterator.instance();
    }

    // TODO: check why not working
    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        if (edgeLabels.length == 0)
            return edgeControllers.entrySet().stream()
                    .flatMap(edgeControllers1 ->
                    edgeControllers1.getValue().stream().map(edgeController -> {
                                Predicates p = new Predicates(predicates);
                                p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeControllers1.getKey())));
                                p.labels.add(edgeControllers1.getKey());
                                return edgeController.edges(vertices, direction, edgeLabels, p);
                            }
                        ).collect(Collectors.toList()).stream())
                    .flatMap(baseEdgeIterator -> {
                        Iterable<BaseEdge> iterable = () -> baseEdgeIterator;
                        return StreamSupport.stream(iterable.spliterator(), false);
                    }).collect(Collectors.toList()).iterator();
        else
            return Stream.of(edgeLabels).flatMap(label ->
                    edgeControllers.get(label).stream()
                            .map(edgeController -> {
                                Predicates p = new Predicates(predicates);
                                for (HasContainer hasContainer : p.hasContainers) {
                                    if (hasContainer.getKey().equals(T.label.getAccessor())) {
                                        p.hasContainers.remove(hasContainer);
                                        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(label)));
                                        break;
                                    }
                                }
                                return edgeController.edges(vertices, direction, edgeLabels, p);
                            })
                            .collect(Collectors.toList()).stream()
            ).flatMap(baseEdgeIterator -> {
                Iterable<BaseEdge> iterable = () -> baseEdgeIterator;
                return StreamSupport.stream(iterable.spliterator(), false);
            }).collect(Collectors.toList()).iterator();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return edgeControllers.get(label).stream().findFirst().get().addEdge(edgeId, label, outV, inV, properties);
    }

    private Object getLabel(Predicates predicates) {
        for (HasContainer hasContainer : predicates.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()))
                return hasContainer.getValue();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        Object labels = getLabel(predicates);
        if (labels == null)
            return vertexControllers.entrySet().stream()
                    .flatMap(vertexControllers1 ->
                            vertexControllers1.getValue().stream().map(vertexController -> {
                                        Predicates p = new Predicates(predicates);
                                        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(vertexControllers1.getKey())));
                                        p.labels.add(vertexControllers1.getKey());
                                        return vertexController.vertices(p);
                                    }
                            ).collect(Collectors.toList()).stream()
                    ).flatMap(baseVertexIterator -> {
                        Iterable<BaseVertex> iterable = () -> baseVertexIterator;
                        return StreamSupport.stream(iterable.spliterator(), false);
                    }).collect(Collectors.toList()).iterator();
        else if (labels instanceof String)
            return vertexControllers.get(labels.toString()).stream()
                    .map(vertexController -> vertexController.vertices(predicates))
                    .flatMap(baseVertexIterator -> {
                        Iterable<BaseVertex> iterable = () -> baseVertexIterator;
                        return StreamSupport.stream(iterable.spliterator(), false);
                    }).collect(Collectors.toList()).iterator();
        else if (labels instanceof Iterable)
            return StreamSupport.stream(((Iterable<String>) labels).spliterator(), false).flatMap(label ->
                    vertexControllers.get(label).stream()
                            .map(vertexController -> {
                                Predicates p = new Predicates(predicates);
                                for (HasContainer hasContainer : p.hasContainers) {
                                    if (hasContainer.getKey().equals(T.label.getAccessor())) {
                                        p.hasContainers.remove(hasContainer);
                                        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(label)));
                                        break;
                                    }
                                }
                                return vertexController.vertices(p);
                            })
                            .collect(Collectors.toList()).stream()
            ).flatMap(baseVertexIterator -> {
                Iterable<BaseVertex> iterable = () -> baseVertexIterator;
                return StreamSupport.stream(iterable.spliterator(), false);
            }).collect(Collectors.toList()).iterator();

        return EmptyIterator.instance();
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return vertexControllers.get(vertexLabel).stream()
                .flatMap(vertexController -> Stream.of(vertexController.vertex(direction, vertexId, vertexLabel)))
                .findFirst().get();
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return vertexControllers.get(label).stream().findFirst().get().addVertex(id, label, properties);
    }

    @Override
    public long edgeCount(Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public long vertexCount(Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

}
