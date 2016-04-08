package org.unipop.process.vertex;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.unipop.query.StepDescriptor;
import org.unipop.process.predicate.ReceivesHasContainers;
import org.unipop.query.controller.ControllerManager;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.common.util.StreamUtils;
import org.unipop.query.search.SearchVertexQuery;

import java.util.*;

public class UniGraphVertexStep extends AbstractStep<Vertex, Edge> implements ReceivesHasContainers<Vertex, Edge> {
    private int limit;
    private final Direction direction;
    private ArrayList<HasContainer> hasContainers = new ArrayList<>();

    private final StepDescriptor stepDescriptor;
    protected final ControllerManager controllerManager;
    private final List<SearchVertexQuery.SearchVertexController> controllers;
    private final int bulk;
    private Iterator<Traverser<Edge>> results;

    public UniGraphVertexStep(VertexStep vertexStep, ControllerManager controllerManager) {
        super(vertexStep.getTraversal());
        vertexStep.getLabels().forEach(label->this.addLabel(label.toString()));
        this.direction = vertexStep.getDirection();
        HasContainer labelsPredicate = new HasContainer(T.label.getAccessor(), P.within(vertexStep, vertexStep.getEdgeLabels()));
        this.hasContainers.add(labelsPredicate);

        this.stepDescriptor = new StepDescriptor(this);
        this.controllerManager = controllerManager;
        this.controllers = this.controllerManager.getControllers(SearchVertexQuery.SearchVertexController.class);
        this.bulk = getTraversal().getGraph().get().configuration().getInt("bulk", 100);
    }

    @Override
    protected Traverser<Edge> processNextStart() {
        if(results == null)
            results = query();

        if(results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser<Edge>> query() {
        UnmodifiableIterator<List<Traverser.Admin<Vertex>>> partitionedTraversers = Iterators.partition(starts, bulk);
        return StreamUtils.asStream(partitionedTraversers).<Iterator<Traverser<Edge>>>map(traverserPartition -> {
            Map<Object, Traverser<Vertex>> traversers = new HashMap<>(bulk);
            List<Vertex> vertices = new ArrayList<>(bulk);
            traverserPartition.forEach(traverser -> {
                traversers.put(traverser.get().id(), traverser);
                vertices.add(traverser.get());
            });
            SearchVertexQuery vertexQuery = new SearchVertexQuery(Edge.class, vertices, direction, hasContainers, limit, stepDescriptor);
            return controllers.stream().<Iterator<Edge>>map(controller -> controller.query(vertexQuery))
                    .<Edge>flatMap(StreamUtils::asStream)
                    .<Iterator<Traverser<Edge>>>map(edge -> {
                        Traverser<Vertex> outVertexTraverser = traversers.get(edge.outVertex().id());
                        Traverser<Vertex> inVertexTraverser = traversers.get(edge.inVertex().id());
                        Traverser<Edge> firstEdge = null;
                        Traverser<Edge> secondEdge = null;
                        if (!direction.equals(Direction.IN) && outVertexTraverser != null) {
                            firstEdge = outVertexTraverser.asAdmin().split(edge, this);
                        }
                        if (!direction.equals(Direction.OUT) && inVertexTraverser != null) {
                            secondEdge = inVertexTraverser.asAdmin().split(edge, this);
                        }

                        if (firstEdge != null && secondEdge != null) {
                            if (outVertexTraverser.get().id().equals(inVertexTraverser.get().id()))
                                return Iterators.singletonIterator(firstEdge);
                            else return Iterators.forArray(firstEdge, secondEdge);
                        } else if (firstEdge != null) return Iterators.singletonIterator(firstEdge);
                        else if (secondEdge != null) return Iterators.singletonIterator(secondEdge);
                        else return null;
                    })
                    .<Traverser<Edge>>flatMap(StreamUtils::asStream).iterator();
        }).<Traverser<Edge>>flatMap(StreamUtils::asStream).iterator();

    }

    public static Vertex vertexToVertex(Vertex originalVertex, Edge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if(outV.id().equals(inV.id()))
                    return originalVertex; //points to self
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
        return StringFactory.stepString(this, this.getDirection(), this.hasContainers);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }


    public Direction getDirection() {
        return direction;
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return hasContainers;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }
}
