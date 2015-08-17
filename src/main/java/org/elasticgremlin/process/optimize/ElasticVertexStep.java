package org.elasticgremlin.process.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.structure.BaseVertex;

import java.util.*;

public class ElasticVertexStep<E extends Element> extends AbstractStep<Vertex, E> {
    protected final Predicates predicates;
    protected final QueryHandler queryHandler;
    private final Direction direction;
    private final Class returnClass;
    private final String[] edgeLabels;
    private Iterator<Traverser<E>> results = EmptyIterator.instance();

    public ElasticVertexStep(VertexStep vertexStep, Predicates predicates, QueryHandler queryHandler) {
        super(vertexStep.getTraversal());
        this.direction = vertexStep.getDirection();
        this.returnClass = vertexStep.getReturnClass();
        this.predicates = predicates;
        this.queryHandler = queryHandler;
        vertexStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(label -> this.addLabel(label.toString()));
        this.edgeLabels = vertexStep.getEdgeLabels();
        if(edgeLabels.length > 0)
            this.predicates.hasContainers.add(new HasContainer("~label", P.within(edgeLabels)));
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (!results.hasNext() && starts.hasNext()) {
            List<Traverser.Admin<Vertex>> traversers = new ArrayList<>();
            for(int i=0; i < 100 && starts.hasNext(); i++)
                traversers.add(starts.next());
            results = query(traversers);
        }
        if(results.hasNext())
            return results.next();
        else throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser<E>> query(List<Traverser.Admin<Vertex>> traversers) {
        Set<Vertex> vertexIds = new HashSet<>();
        traversers.forEach(traverser->vertexIds.add(traverser.get()));
        Iterator<Edge> edgeIterator = queryHandler.edges(vertexIds.iterator(), direction, edgeLabels, predicates);

        Map<Object, ArrayList<E>> idToResults = new HashMap<>();
        edgeIterator.forEachRemaining(edge -> edge.vertices(direction).forEachRemaining(vertex -> {
            ArrayList<E> list = idToResults.get(vertex.id());
            if(list == null || !(list instanceof List)) {
                list = new ArrayList();
                idToResults.put(vertex.id(), list);
            }
            Element element = !Vertex.class.equals(returnClass) ? edge : BaseVertex.vertexToVertex(vertex, edge, direction);
            list.add((E) element);
        }));

        List<Traverser<E>> returnTraversers = new ArrayList<>();
        traversers.forEach(traverser -> {
            ArrayList<E> list = idToResults.get(traverser.get().id().toString());
            if (list != null) for (E element : list)
                returnTraversers.add(traverser.split(element, this));
        });
        return returnTraversers.iterator();
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
