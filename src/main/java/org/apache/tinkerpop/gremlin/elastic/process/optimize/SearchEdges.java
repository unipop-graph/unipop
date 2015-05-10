package org.apache.tinkerpop.gremlin.elastic.process.optimize;

import org.apache.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import org.apache.tinkerpop.gremlin.elastic.structure.*;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public class SearchEdges<E extends Element> extends ElasticFlatMapStep<Vertex,E> {

    private final boolean returnVertex;
    private String[] edgeLabels;
    public SearchEdges(VertexStep vertexStep, ArrayList<HasContainer> hasContainers, ElasticService elasticService, Integer resultsLimit) {
        super(vertexStep.getTraversal(), vertexStep.getLabel(), elasticService, hasContainers, vertexStep.getDirection() ,resultsLimit);
        this.edgeLabels = vertexStep.getEdgeLabels();
        if(this.edgeLabels.length > 0)
            this.hasContainers.add(new HasContainer("~label", Contains.within, edgeLabels));
        this.returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
    }

    @Override
    protected Map<Traverser.Admin<Vertex>, ArrayList<E>> query(List<Traverser.Admin<Vertex>> traversers) {
        HashMap<String, List<Traverser.Admin<Vertex>>> vertexIdToTraverser = new HashMap<>();
        traversers.forEach(traverser -> putOrAddToList(vertexIdToTraverser, traverser.get().id(), traverser));

        Iterator<Edge> edgeIterator = elasticService.searchEdges(hasContainers, resultsLimit, direction,
                vertexIdToTraverser.keySet().toArray(new String[vertexIdToTraverser.keySet().size()]));

        Map<Traverser.Admin<Vertex>, ArrayList<E>> results = new HashMap<>();
        edgeIterator.forEachRemaining(edge ->
                ((ElasticEdge) edge).getVertexId(direction).forEach(vertexKey ->
                        vertexIdToTraverser.get(vertexKey).forEach(traverser ->
                                putOrAddToList(results, traverser, !returnVertex ? edge :
                                        ((ElasticVertex)traverser.get()).vertexToVertex((ElasticEdge) edge, direction)))));

        return results;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels));
    }
}
