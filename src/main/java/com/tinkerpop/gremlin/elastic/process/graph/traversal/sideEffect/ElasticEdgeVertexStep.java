package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.index.query.BoolFilterBuilder;

import java.util.*;

public class ElasticEdgeVertexStep extends ElasticFlatMapStep<Edge,Vertex> {

    public ElasticEdgeVertexStep(EdgeVertexStep originalStep, BoolFilterBuilder boolFilter, String[] labels, ElasticService elasticService) {
        super(originalStep.getTraversal(),elasticService, boolFilter, labels, originalStep.getDirection());
    }

    @Override
    protected void load(Iterator<ElasticTraverser> traversers) {
        Map<String,List<ElasticTraverser>> vertexIdToTraverser =  new HashMap<>();
        traversers.forEachRemaining(traver -> ((ElasticEdge) traver.getElement()).getVertexId(direction).forEach(id -> {
            List<ElasticTraverser> traverserList = vertexIdToTraverser.get(id);
            if (traverserList == null) traverserList = new ArrayList<>();
            traverserList.add(traver);
        }));

        Object[] allVertexIds = vertexIdToTraverser.keySet().toArray();
        Iterator<Vertex> vertexIterator = elasticService.searchVertices(boolFilter, allVertexIds, labels);

        vertexIterator.forEachRemaining(vertex ->
            vertexIdToTraverser.get(vertex.id()).forEach(traverser ->
                    traverser.addResult(vertex)));
    }
}
