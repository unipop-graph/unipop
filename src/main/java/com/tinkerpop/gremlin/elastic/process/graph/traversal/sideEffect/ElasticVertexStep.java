package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.index.query.*;

import java.util.*;

public class ElasticVertexStep<E extends Element> extends ElasticFlatMapStep<Vertex,E> {

    private final Class returnClass;
    private final String[] typeLabels;
    private String[] edgeLabels;
    private Object[] onlyAllowedIds;
    public ElasticVertexStep(VertexStep originalStep, BoolFilterBuilder boolFilter, String[] typeLabels,Object[] onlyAllowedIds, ElasticService elasticService) {
        super(originalStep.getTraversal(), originalStep.getLabel(), elasticService, boolFilter, originalStep.getDirection());
        this.typeLabels = typeLabels;
        this.edgeLabels = originalStep.getEdgeLabels();
        this.onlyAllowedIds = onlyAllowedIds;
        returnClass = originalStep.getReturnClass();
    }

    @Override
    protected void load(List<ElasticTraverser> traversers) {
        if(returnClass.isAssignableFrom(Vertex.class))
             loadVertices(traversers);
        else loadEdges(traversers, boolFilter);
    }

    private void loadEdges(List<ElasticTraverser> traversers, BoolFilterBuilder filter) {
        HashMap<String, List<ElasticTraverser>> vertexIdToTraverser = new HashMap<>();
        traversers.forEach(traverser -> {
            String id = traverser.getElement().id().toString();
            List<ElasticTraverser> traverserList = vertexIdToTraverser.get(id);
            if (traverserList == null) {
                traverserList = new ArrayList<>();
                vertexIdToTraverser.put(id, traverserList);
            }
            traverserList.add(traverser);

        });

        Object[] allVertexIds = vertexIdToTraverser.keySet().toArray();
        if(direction == Direction.IN) filter.must(FilterBuilders.termsFilter(ElasticEdge.InId, allVertexIds));
        else if(direction == Direction.OUT) filter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, allVertexIds));
        else if(direction == Direction.BOTH) filter.should(FilterBuilders.termsFilter(ElasticEdge.InId, allVertexIds), FilterBuilders.termsFilter(ElasticEdge.OutId, allVertexIds));
        else throw new EnumConstantNotPresentException(direction.getClass(),direction.name());

        Iterator<Edge> edgeIterator = elasticService.searchEdges(filter, null, edgeLabels);

        edgeIterator.forEachRemaining(edge ->
                ((ElasticEdge) edge).getVertexId(direction).forEach(vertexKey ->
                {
                    if(vertexIdToTraverser.get(vertexKey)!=null)
                    vertexIdToTraverser.get(vertexKey).forEach(traverser -> traverser.addResult((E) edge));
                }));
    }

    private void loadVertices(List<ElasticTraverser> traversers) {
        loadEdges(traversers, FilterBuilders.boolFilter());//perdicates belong to vertices query

        Map<String,List<ElasticTraverser>> vertexIdToTraversers = new HashMap<>();
        traversers.forEach(traverser -> {
            traverser.getResults().forEach(edge -> {
                ElasticEdge elasticEdge = (ElasticEdge) edge;
                elasticEdge.getVertexId(direction.opposite()).forEach(id -> {
                    if(id == traverser.getElement() && //disregard self in Direction.BOTH traversals
                        direction.equals(Direction.BOTH) &&
                        elasticEdge.getVertexId(Direction.IN) != elasticEdge.getVertexId(Direction.OUT)) // except when the vertex points to itself
                        return;

                    List<ElasticTraverser> traverserList = vertexIdToTraversers.get(id);
                    if (traverserList == null) {
                        traverserList = new ArrayList<>();
                        vertexIdToTraversers.put(id.toString(), traverserList);
                    }
                traverserList.add(traverser);
                });
            });
            traverser.clearResults();
        });

        Object[] allVertexIds = onlyAllowedIds.length > 0? onlyAllowedIds : vertexIdToTraversers.keySet().toArray();

        Iterator<Vertex> vertexIterator = elasticService.searchVertices(boolFilter, allVertexIds, typeLabels);

        vertexIterator.forEachRemaining(vertex -> {
            List<ElasticTraverser> elasticTraversers = vertexIdToTraversers.get(vertex.id());
            if(elasticTraversers != null) elasticTraversers.forEach(traverser -> traverser.addResult((E) vertex));
        });
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }
}
