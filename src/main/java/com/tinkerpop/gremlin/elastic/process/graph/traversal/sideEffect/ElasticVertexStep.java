package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.index.query.*;

import java.util.*;

public class ElasticVertexStep<E extends Element> extends ElasticFlatMapStep<Vertex,E> {

    private final Class returnClass;
    private String[] edgeLabels;

    public ElasticVertexStep(VertexStep originalStep, BoolFilterBuilder boolFilter, String[] typeLabels, ElasticService elasticService) {
        super(originalStep.getTraversal(), elasticService, boolFilter, typeLabels, originalStep.getDirection());
        this.edgeLabels = originalStep.getEdgeLabels();
        returnClass = originalStep.getReturnClass();
    }

    @Override
    protected void load(Iterator<ElasticTraver> traversers) {
        if(returnClass.isAssignableFrom(Vertex.class))
             loadVertices(traversers);
        else loadEdges(traversers, boolFilter);
    }

    private void loadEdges(Iterator<ElasticTraver> traversers, BoolFilterBuilder filter) {
        HashMap<String, List<ElasticTraver>> vertexIdToTraverser = new HashMap<>();
        traversers.forEachRemaining(traverser -> {
            String id = traverser.getElement().id().toString();
            List<ElasticTraver> traverserList = vertexIdToTraverser.get(id);
            if (traverserList == null) traverserList = new ArrayList<>();
            traverserList.add(traverser);
        });

        Object[] allVertexIds = vertexIdToTraverser.keySet().toArray();
        if(direction == Direction.IN) filter.must(FilterBuilders.termsFilter(ElasticEdge.InId, allVertexIds));
        else if(direction == Direction.OUT) filter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, allVertexIds));
        else if(direction == Direction.BOTH) filter.should(FilterBuilders.termsFilter(ElasticEdge.InId, allVertexIds), FilterBuilders.termsFilter(ElasticEdge.OutId, allVertexIds));
        else throw new EnumConstantNotPresentException(direction.getClass(),direction.name());

        Iterator<Edge> edgeIterator = elasticService.searchEdges(filter, null, labels);

        edgeIterator.forEachRemaining(edge ->
                ((ElasticEdge) edge).getVertexId(direction).forEach(vertexKey ->
                        vertexIdToTraverser.get(vertexKey).forEach(traverser -> traverser.addResult((E) edge))));
    }

    private void loadVertices(Iterator<ElasticTraver> traversers) {
        loadEdges(traversers, FilterBuilders.boolFilter());//perdicates belong to vertices query

        Map<String,List<ElasticTraver>> vertexIdToTraversers = new HashMap<>();
        traversers.forEachRemaining(traverser -> {
            traverser.clearResults();
            traverser.getResults().forEach(edge ->
                ((ElasticEdge) edge).getVertexId(direction).forEach(id -> {
                    List<ElasticTraver> traverserList = vertexIdToTraversers.get(id);
                    if (traverserList == null) traverserList = new ArrayList<>();
                    traverserList.add(traverser);
                    vertexIdToTraversers.put(id.toString(), traverserList);
                }));
        });

        Object[] allVertexIds = vertexIdToTraversers.keySet().toArray();
        Iterator<Vertex> vertexIterator = elasticService.searchVertices(boolFilter, allVertexIds, labels);

        vertexIterator.forEachRemaining(vertex -> vertexIdToTraversers.get(vertex.id()).forEach(traverser -> traverser.addResult((E) vertex)));
    }
}
