package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.structure.*;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;

public class ElasticVertexStep<E extends Element> extends VertexStep<E> {

    protected final ArrayList<HasContainer> hasContainers;
    protected final ElasticService elasticService;
    protected final Integer resultsLimit;
    private Map<String, ArrayList<E>> results;

    public ElasticVertexStep(VertexStep vertexStep, ArrayList<HasContainer> hasContainers, ElasticService elasticService, Integer resultsLimit) {
        super(vertexStep.getTraversal(), vertexStep.getReturnClass(), vertexStep.getDirection(), vertexStep.getEdgeLabels());
        this.hasContainers = hasContainers;
        this.elasticService = elasticService;
        this.resultsLimit = resultsLimit;
        vertexStep.getLabels().forEach(label -> this.addLabel(label.toString()));
        if(this.getEdgeLabels().length > 0)
            this.hasContainers.add(new HasContainer("~label", Contains.within, this.getEdgeLabels()));
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (results == null) {
            if (!starts.hasNext()) throw FastNoSuchElementException.instance();
            List<Traverser.Admin<Vertex>> traversers = new ArrayList<>();
            starts.forEachRemaining(traversers::add);
            starts.add(traversers.iterator());
            Set<String> ids = new HashSet<>();
            traversers.forEach(traverser->ids.add(traverser.get().id().toString()));
            results = query(ids);
        }
        return super.processNextStart();
    }


    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        ArrayList<E> returnValue = results.get(traverser.get().id().toString());
        if(returnValue != null)
            return returnValue.iterator();
        return EmptyIterator.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.results = null;
    }

    private Map<String, ArrayList<E>> query(Set<String> vertexIds) {

        Map<String, ArrayList<E>> results = new HashMap<>();
        switch (getDirection()) {
            case IN:
                runQuery(vertexIds, results, Direction.IN);
                break;
            case OUT:
                runQuery(vertexIds, results, Direction.OUT);
                break;
            case BOTH:
                runQuery(vertexIds, results, Direction.IN);
                runQuery(vertexIds, results, Direction.OUT);
                break;
        }
        return results;
    }

    private void runQuery(Set<String> ids, Map<String, ArrayList<E>> results, Direction direction) {
        Iterator<Edge> edgeIterator = elasticService.searchEdges(hasContainers, resultsLimit, direction, ids.toArray());

        boolean returnVertex = getReturnClass().equals(Vertex.class);
        edgeIterator.forEachRemaining(edge -> edge.vertices(direction).forEachRemaining(vertex ->
            putOrAddToList(results, vertex.id(), !returnVertex ? edge : ElasticVertex.vertexToVertex(vertex, (ElasticEdge) edge, getDirection()))));
    }

    protected void putOrAddToList(Map map, Object key, Object value) {
        Object list = map.get(key);
        if(list == null || !(list instanceof List)) {
            list = new ArrayList();
            map.put(key, list);
        }
        ((List)list).add(value);
    }
}
