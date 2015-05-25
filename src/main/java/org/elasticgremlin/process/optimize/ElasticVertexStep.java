package org.elasticgremlin.process.optimize;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticservice.*;
import org.elasticgremlin.structure.*;

import java.util.*;

public class ElasticVertexStep<E extends Element> extends AbstractStep<Vertex, E> {

    protected final Predicates predicates;
    protected final ElasticService elasticService;
    private final Direction direction;
    private final boolean returnVertex;
    private Iterator<Traverser<E>> results;

    public ElasticVertexStep(VertexStep vertexStep, Predicates predicates, ElasticService elasticService) {
        super(vertexStep.getTraversal());
        this.direction = vertexStep.getDirection();
        this.returnVertex = Vertex.class.equals(vertexStep.getReturnClass());
        this.predicates = predicates;
        this.elasticService = elasticService;
        vertexStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        predicates.labels.forEach(label -> this.addLabel(label.toString()));
        if(vertexStep.getEdgeLabels().length > 0)
            this.predicates.hasContainers.add(new HasContainer("~label", P.within(vertexStep.getEdgeLabels())));
    }

    @Override
    protected Traverser<E> processNextStart() {
        if ((results == null || !results.hasNext()) && starts.hasNext()) {
            List<Traverser.Admin<Vertex>> traversers = new ArrayList<>();
            starts.forEachRemaining(traversers::add);
            results = query(traversers);
        }
        if(results != null && results.hasNext())
            return results.next();
        else throw FastNoSuchElementException.instance();
    }

    private Iterator<Traverser<E>> query(List<Traverser.Admin<Vertex>> traversers) {

        Set<String> vertexIds = new HashSet<>();
        traversers.forEach(traverser->vertexIds.add(traverser.get().id().toString()));
        Iterator<Edge> edgeIterator = elasticService.searchEdges(predicates, direction, vertexIds.toArray());

        Map<String, ArrayList<E>> idToResults = new HashMap<>();
        edgeIterator.forEachRemaining(edge -> edge.vertices(direction).forEachRemaining(vertex -> putOrAddToList(idToResults, vertex.id(), !returnVertex ? edge : ElasticVertex.vertexToVertex(vertex, (ElasticEdge) edge, direction))));

        List<Traverser<E>> returnTraversers = new ArrayList<>();
        traversers.forEach(traverser -> {
            ArrayList<E> list = idToResults.get(traverser.get().id().toString());
            if (list != null) for (E element : list)
                returnTraversers.add(traverser.split(element, this));
        });
        return returnTraversers.iterator();
    }

    protected void putOrAddToList(Map map, Object key, Object value) {
        Object list = map.get(key);
        if(list == null || !(list instanceof List)) {
            list = new ArrayList();
            map.put(key, list);
        }
        ((List)list).add(value);
    }

    @Override
    public void reset() {
        super.reset();
        this.results = null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
