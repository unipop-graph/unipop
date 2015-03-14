package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Created by Eliran on 11/3/2015.
 */
public class EdgeSearchStep extends ElasticSearchFlatMap<Vertex,Edge> {

    private Direction direction;
    private List<String> edgeLabels;
    public EdgeSearchStep(Traversal traversal,Direction direction,ElasticService elasticService, Optional<String> label,String... edgeLabels) {
        super(traversal,elasticService);
        this.direction = direction;
        this.setFunction(traverser ->
                getEdgesIterator(traverser) );
        if(label.isPresent()){
            this.setLabel(label.get());
        }
        if(edgeLabels!=null) {
            this.edgeLabels = Arrays.asList(edgeLabels);
        }
        else {
            this.edgeLabels = new ArrayList<>();
        }
    }

    private Iterator<Edge> getEdgesIterator(Iterator<Vertex> vertexIterator) {
        List<String> originalVerticesIds = new ArrayList<String>();
        HashMap<String,List<String>> vertexIdToEdgesResults = new HashMap<String,List<String>>();
        vertexIterator.forEachRemaining(vertex -> {
            this.addId(vertex.id());
            String vertexIdString = vertex.id().toString();
            originalVerticesIds.add(vertexIdString);

        });
        Iterator<Edge> edgeIterator = elasticService.searchEdges(FilterBuilderProvider.getFilter(this, direction), edgeLabels.toArray(new String[edgeLabels.size()]));
        Map<String,Edge> edgeIdToEdge = new HashMap<String,Edge>();
        edgeIterator.forEachRemaining(edge -> {
            String edgeIdString = edge.id().toString();
            edgeIdToEdge.put(edgeIdString,edge);
            ElasticEdge elasticEdge = (ElasticEdge) edge;
            List<String> vertexKeys = elasticEdge.getVertexId(direction);
            for(String vertexKey : vertexKeys){
                List<String> cacheList;
                if(!vertexIdToEdgesResults.containsKey(vertexKey)){
                    cacheList = new ArrayList<String>();
                    vertexIdToEdgesResults.put(vertexKey,cacheList);
                }
                else cacheList = vertexIdToEdgesResults.get(vertexKey);
                cacheList.add(edgeIdString);
            }

        });

        return addJumpingPointsAndReturnCorrectedIterator(originalVerticesIds,vertexIdToEdgesResults,edgeIdToEdge);
    }




    private Iterator<Edge> addJumpingPointsAndReturnCorrectedIterator(List<String> inputIds,Map<String,List<String>> inputIdToResultSet,Map<String,Edge> edgeIdToEdge){

        List<Edge> edges = new ArrayList<Edge>();
        int counter = 0 ;
        for(String fromVertexId : inputIds){
            if(inputIdToResultSet.containsKey(fromVertexId)) {
                List<String> toEdges = inputIdToResultSet.get(fromVertexId);
                for (String edgeId : toEdges) {
                    if(edgeIdToEdge.containsKey(edgeId)) {
                        edges.add(edgeIdToEdge.get(edgeId));
                        counter++;
                    }
                }
            }
            this.jumpingPoints.add(counter);
        }
        return edges.iterator();

    }

    @Override
    public void setTypeLabel(String label){
        edgeLabels.add(label);
    }


    @Override
    public String toString(){
        return TraversalHelper.makeStepString(this, this.direction,Arrays.asList(this.edgeLabels), this.getPredicates());
    }
}
