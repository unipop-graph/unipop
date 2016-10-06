package org.unipop.process.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.process.UniPredicatesStep;
import org.unipop.process.UniQueryStep;
import org.unipop.process.order.Orderable;
import org.unipop.query.StepDescriptor;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.query.UniQuery;
import org.unipop.util.ConversionUtils;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UniGraphVertexStep<E extends Element> extends UniPredicatesStep<Vertex, E> implements ReceivesPredicatesHolder<Vertex, E>, Orderable, UniQueryStep<Vertex>, Profiling{
    private static final Logger logger = LoggerFactory.getLogger(UniGraphVertexStep.class);

    private boolean returnsVertex;
    private final Direction direction;
    private Class<E> returnClass;
    private String[] edgeLabels = new String[0];
    private int limit;
    private PredicatesHolder predicates = PredicatesHolderFactory.empty();
    private StepDescriptor stepDescriptor;
    private List<SearchVertexQuery.SearchVertexController> controllers;
    private List<DeferredVertexQuery.DeferredVertexController> deferredVertexControllers;
    private List<Pair<String, Order>> orders;

    public UniGraphVertexStep(VertexStep<E> vertexStep, UniGraph graph, ControllerManager controllerManager) {
        super(vertexStep.getTraversal(), graph);
        vertexStep.getLabels().forEach(this::addLabel);
        this.direction = vertexStep.getDirection();
        this.returnClass = vertexStep.getReturnClass();
        this.returnsVertex = vertexStep.returnsVertex();
        if (vertexStep.getEdgeLabels().length > 0) {
            this.edgeLabels = vertexStep.getEdgeLabels();
            HasContainer labelsPredicate = new HasContainer(T.label.getAccessor(), P.within(vertexStep.getEdgeLabels()));
            this.predicates = PredicatesHolderFactory.predicate(labelsPredicate);
        } else this.predicates = PredicatesHolderFactory.empty();
        this.controllers = controllerManager.getControllers(SearchVertexQuery.SearchVertexController.class);
        this.deferredVertexControllers = controllerManager.getControllers(DeferredVertexQuery.DeferredVertexController.class);
        this.stepDescriptor = new StepDescriptor(this);
        limit = -1;
    }

    public void setReturnClass(Class returnClass){
        this.returnClass = returnClass;
        this.returnsVertex = returnClass == Vertex.class;
    }

    @Override
    public boolean hasNext() {
        return controllers.size() > 0 && super.hasNext();
    }

    @Override
    protected Iterator<Traverser.Admin<E>> process(List<Traverser.Admin<Vertex>> traversers) {
        Map<Object, List<Traverser<Vertex>>> idToTraverser = new HashMap<>(traversers.size());
        List<Vertex> vertices = new ArrayList<>(traversers.size());
        traversers.forEach(traverser -> {
            Vertex vertex = traverser.get();
            List<Traverser<Vertex>> traverserList = idToTraverser.get(vertex.id());
            if (traverserList == null) {
                traverserList = new ArrayList<>(1);
                idToTraverser.put(vertex.id(), traverserList);
            }
            traverserList.add(traverser);
            vertices.add(vertex);
        });
        SearchVertexQuery vertexQuery = (SearchVertexQuery) getQuery(traversers);
        logger.debug("Executing query: {}", vertexQuery.toString());
        SearchVertexQuery vertexQuery;
        if (!returnsVertex)
            vertexQuery = new SearchVertexQuery(Edge.class, vertices, direction, predicates, limit, propertyKeys, orders, stepDescriptor, traversal);
        else
            vertexQuery = new SearchVertexQuery(Edge.class, vertices, direction, predicates, -1, propertyKeys, null, stepDescriptor, traversal);
        logger.debug("Executing query: ", vertexQuery);
        Iterator<Traverser.Admin<E>> traversersIterator = controllers.stream().<Iterator<Edge>>map(controller -> controller.search(vertexQuery))
                .<Edge>flatMap(ConversionUtils::asStream)
                .<Traverser.Admin<E>>flatMap(edge -> toTraversers(edge, idToTraverser)).iterator();
        if (!this.returnsVertex || (propertyKeys != null && propertyKeys.size() == 0))
            return traversersIterator;
        return getTraversersWithProperties(traversersIterator);
    }

    private Iterator<Traverser.Admin<E>> getTraversersWithProperties(Iterator<Traverser.Admin<E>> traversers) {
        List<Traverser.Admin<E>> copyTraversers = ConversionUtils.asStream(traversers).collect(Collectors.toList());
        List<DeferredVertex> deferredVertices = copyTraversers.stream().map(Attachable::get)
                .filter(vertex -> vertex instanceof DeferredVertex)
                .map(vertex -> ((DeferredVertex) vertex))
                .filter(DeferredVertex::isDeferred)
                .collect(Collectors.toList());
        if (deferredVertices.size() > 0) {
            DeferredVertexQuery query = new DeferredVertexQuery(deferredVertices, propertyKeys, orders, this.stepDescriptor, traversal);
            deferredVertexControllers.stream().forEach(controller -> controller.fetchProperties(query));
        }
        return copyTraversers.iterator();
    }

    private Stream<Traverser.Admin<E>> toTraversers(Edge edge, Map<Object, List<Traverser<Vertex>>> traversers) {
        return ConversionUtils.asStream(edge.vertices(direction))
                .<Traverser.Admin<E>>flatMap(originalVertex -> {
                    List<Traverser<Vertex>> vertexTraversers = traversers.get(originalVertex.id());
                    if (vertexTraversers == null) return null;
                    return vertexTraversers.stream().map(vertexTraverser -> {
                        E result = getReturnElement(edge, originalVertex);
                        return vertexTraverser.asAdmin().split(result, this);
                    });
                }).filter(result -> result != null);
    }

    private E getReturnElement(Edge edge, Vertex originalVertex) {
        if (!this.returnsVertex) return (E) edge;
        return (E) UniVertex.vertexToVertex(originalVertex, edge, this.direction);
    }

    @Override
    public void reset() {
        super.reset();
        this.results = EmptyIterator.instance();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void addPredicate(PredicatesHolder predicatesHolder) {
        this.predicates = PredicatesHolderFactory.and(this.predicates, predicatesHolder);
    }

    @Override
    public PredicatesHolder getPredicates() {
        return predicates;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        this.stepDescriptor = new StepDescriptor(this, metrics);
    }

    @Override
    public void setOrders(List<Pair<String, Order>> orders) {
        this.orders = orders;
    }

    public void setControllers(List<SearchVertexQuery.SearchVertexController> controllers) {
        this.controllers = controllers;
    }

    public boolean isReturnsVertex() {
        return returnsVertex;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public UniQuery getQuery(List<Traverser.Admin<Vertex>> traversers) {
        if (!returnsVertex)
            return new SearchVertexQuery(Edge.class, traversers.stream().map(Traverser::get).collect(Collectors.toList()), direction, predicates, limit, propertyKeys, orders, stepDescriptor);
        else
            return new SearchVertexQuery(Edge.class, traversers.stream().map(Traverser::get).collect(Collectors.toList()), direction, predicates, -1, propertyKeys, null, stepDescriptor);
    }

    @Override
    public boolean hasControllers() {
        return controllers.size() > 0;
    }
}
