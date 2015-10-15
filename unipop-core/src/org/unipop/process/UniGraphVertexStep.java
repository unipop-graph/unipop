package org.unipop.process;

import org.unipop.controller.Predicates;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.controllerprovider.ControllerProvider;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.*;

public class UniGraphVertexStep<E extends Element> extends AbstractStep<Vertex, E> {
    protected final Predicates predicates;
    protected final ControllerProvider controllerProvider;
    private final Direction direction;
    private final Class returnClass;
    private final String[] edgeLabels;
    private MutableMetrics metrics;
    private Iterator<Traverser<E>> results = EmptyIterator.instance();

    public UniGraphVertexStep(VertexStep vertexStep, Predicates predicates, ControllerProvider controllerProvider) {
        super(vertexStep.getTraversal());
        this.direction = vertexStep.getDirection();
        this.returnClass = vertexStep.getReturnClass();
        this.predicates = predicates;
        this.controllerProvider = controllerProvider;
        vertexStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(this::addLabel);
        this.edgeLabels = vertexStep.getEdgeLabels();
        if(edgeLabels.length > 0)
            this.predicates.hasContainers.add(new HasContainer("~label", P.within(edgeLabels)));

        Optional<StandardTraversalMetrics> metrics = this.getTraversal().asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY);
        if(metrics.isPresent()) this.metrics = (MutableMetrics) metrics.get().getMetrics(this.getId());
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (!results.hasNext() && starts.hasNext())
            results = query(starts);

        if(results.hasNext())
            return results.next();

        throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser<E>> query(Iterator<Traverser.Admin<Vertex>> traversers) {
        ResultsContainer results = new ResultsContainer();
        List<Traverser.Admin<Vertex>> copyTraversers = new ArrayList<>();
        List<BaseVertex> vertices = new ArrayList<>();

        while(traversers.hasNext() && vertices.size() <= 100)
        {
            Traverser.Admin<Vertex> traverser = traversers.next();
            vertices.add((BaseVertex) traverser.get());
            copyTraversers.add(traverser);
        }

        results.addResults(controllerProvider.fromVertex(vertices.toArray(new BaseVertex[0]), direction, edgeLabels, predicates, metrics));

        List<Traverser<E>> returnTraversers = new ArrayList<>();
        copyTraversers.forEach(traverser -> {
            ArrayList<E> list = results.get(traverser.get().id().toString());
            if (list != null) for (E element : list)
                returnTraversers.add(traverser.split(element, this));
        });
        return returnTraversers.iterator();
    }

    private class ResultsContainer {
        Map<Object, ArrayList<E>> idToResults = new HashMap<>();

        public void addResults(Iterator<BaseEdge> edgeIterator) {
            edgeIterator.forEachRemaining(edge -> edge.vertices(direction).forEachRemaining(vertex -> {
                ArrayList<E> list = idToResults.get(vertex.id());
                if (list == null || !(list instanceof List)) {
                    list = new ArrayList();
                    idToResults.put(vertex.id(), list);
                }
                Element element = !Vertex.class.equals(returnClass) ? edge : vertexToVertex(vertex, edge, direction);
                list.add((E) element);
            }));
        }

        public ArrayList<E> get(String key) {
            return idToResults.get(key);
        }
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
        return StringFactory.stepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
