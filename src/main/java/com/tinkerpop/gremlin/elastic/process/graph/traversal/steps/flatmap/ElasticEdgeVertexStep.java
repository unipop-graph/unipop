package com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;

import java.util.*;

public class ElasticEdgeVertexStep extends ElasticFlatMapStep<Edge,Vertex> {

    public ElasticEdgeVertexStep(EdgeVertexStep originalStep, ArrayList<HasContainer> hasContainers, ElasticService elasticService, Integer resultsLimit) {
        super(originalStep.getTraversal(), originalStep.getLabel(), elasticService, hasContainers, originalStep.getDirection(), resultsLimit);
    }

    @Override
    protected void load(List<ElasticTraverser> traversers) {
        Map<String,List<ElasticTraverser>> vertexIdToTraverser =  new HashMap<>();
        traversers.forEach(traver -> ((ElasticEdge) traver.getElement()).getVertexId(direction).forEach(id -> {
            List<ElasticTraverser> traverserList = vertexIdToTraverser.get(id);
            if (traverserList == null) {
                traverserList = new ArrayList<>();
                vertexIdToTraverser.put(id.toString(), traverserList);
            }
            traverserList.add(traver);
        }));

        ArrayList<HasContainer> hasList = hasContainers;
        Object[] ids = vertexIdToTraverser.keySet().toArray();
        if(ids.length > 0) {
            hasList  = (ArrayList<HasContainer>) hasContainers.clone();
            hasList.add(new HasContainer("~id", Contains.within, ids));
        }
        Iterator<Vertex> vertexIterator = elasticService.searchVertices(hasList ,resultsLimit);

        vertexIterator.forEachRemaining(vertex ->
            vertexIdToTraverser.get(vertex.id()).forEach(traverser ->
                    traverser.addResult(vertex)));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction);
    }
}
