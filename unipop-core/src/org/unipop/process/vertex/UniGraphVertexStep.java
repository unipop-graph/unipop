package org.unipop.process.vertex;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.unipop.query.StepDescriptor;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.query.controller.ControllerManager;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.common.util.ConversionUtils;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchVertexQuery;

import java.util.*;
import java.util.stream.Stream;

public class UniGraphVertexStep<E extends Element> extends AbstractStep<Vertex, E> implements ReceivesPredicatesHolder<Vertex, E> {
    private final boolean returnsVertex;
    private int limit;
    private final Direction direction;
    private PredicatesHolder predicates = PredicatesHolderFactory.empty();

    private final StepDescriptor stepDescriptor;
    protected final ControllerManager controllerManager;
    private final List<SearchVertexQuery.SearchVertexController> controllers;
    private final int bulk;
    private Iterator<Traverser<E>> results;

    public UniGraphVertexStep(VertexStep<E> vertexStep, ControllerManager controllerManager) {
        super(vertexStep.getTraversal());
        vertexStep.getLabels().forEach(this::addLabel);
        this.direction = vertexStep.getDirection();
        if(vertexStep.getEdgeLabels().length > 0) {
            HasContainer labelsPredicate = new HasContainer(T.label.getAccessor(), P.within(vertexStep.getEdgeLabels()));
            this.predicates = PredicatesHolderFactory.predicate(labelsPredicate);
        }
        else this.predicates = PredicatesHolderFactory.empty();
        this.returnsVertex = vertexStep.returnsVertex();
        this.stepDescriptor = new StepDescriptor(this);
        this.controllerManager = controllerManager;
        this.controllers = this.controllerManager.getControllers(SearchVertexQuery.SearchVertexController.class);
        this.bulk = getTraversal().getGraph().get().configuration().getInt("bulk", 100);
        limit = -1;
    }

    @Override
    protected Traverser<E> processNextStart() {
        if(results == null)
            results = query();

        if(results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser<E>> query() {
        UnmodifiableIterator<List<Traverser.Admin<Vertex>>> partitionedTraversers = Iterators.partition(starts, bulk);
        return ConversionUtils.asStream(partitionedTraversers)
                .<Iterator<Traverser<E>>>map(this::queryBulk)
                .<Traverser<E>>flatMap(ConversionUtils::asStream).iterator();
    }

    private Iterator<Traverser<E>> queryBulk(List<Traverser.Admin<Vertex>> traversers) {
        Map<Object, List<Traverser<Vertex>>> idToTraverser = new HashMap<>(bulk);
        List<Vertex> vertices = new ArrayList<>(bulk);
        traversers.forEach(traverser -> {
            Vertex vertex = traverser.get();
            List<Traverser<Vertex>> traverserList = idToTraverser.get(vertex.id());
            if(traverserList == null) {
                traverserList = new ArrayList<>(1);
                idToTraverser.put(vertex.id(), traverserList);
            }
            traverserList.add(traverser);
            vertices.add(vertex);
        });
        SearchVertexQuery vertexQuery = new SearchVertexQuery(Edge.class, vertices, direction, predicates, limit, stepDescriptor);
        return controllers.stream().<Iterator<Edge>>map(controller -> controller.search(vertexQuery))
                .<Edge>flatMap(ConversionUtils::asStream)
                .<Traverser<E>>flatMap(edge -> toTraversers(edge, idToTraverser)).iterator();
    }

    private Stream<Traverser.Admin<E>> toTraversers(Edge edge, Map<Object, List<Traverser<Vertex>>> traversers) {
        return ConversionUtils.asStream(edge.vertices(direction))
            .<Traverser.Admin<E>>flatMap(originalVertex -> {
                List<Traverser<Vertex>> vertexTraversers = traversers.get(originalVertex.id());
                if(vertexTraversers == null) return null;
                return vertexTraversers.stream().map(vertexTraverser -> {
                    E result = getReturnElement(edge, originalVertex);
                    return vertexTraverser.asAdmin().split(result, this);
                });
            }).filter(result -> result != null);
    }

    private E getReturnElement(Edge edge, Vertex originalVertex) {
        if(!this.returnsVertex) return (E) edge;
        return (E) vertexToVertex(originalVertex, edge, this.direction);
    }

    private Vertex vertexToVertex(Vertex originalVertex, Edge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if(outV.id().equals(inV.id()))
                    return outV; //points to self
                if(originalVertex.id().equals(inV.id()))
                    return outV;
                if(originalVertex.id().equals(outV.id()))
                    return inV;
            default:
                throw new IllegalArgumentException(direction.toString());
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.results = EmptyIterator.instance();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.getDirection(), this.predicates);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }


    public Direction getDirection() {
        return direction;
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
}
