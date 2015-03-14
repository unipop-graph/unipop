package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import java.util.*;

/**
 * Created by Eliran on 11/3/2015.
 */
public class VertexSearchStep<E extends Element> extends ElasticSearchFlatMap<E,Vertex> {


    private Direction direction;
    private Class<E> stepClass;
    private String[] edgeLabels;

    public VertexSearchStep(Traversal traversal, Direction direction, ElasticService elasticService,Class<E> stepClass, Optional<String> label,String... edgeLabels) {
        super(traversal,elasticService);
        this.direction = direction;
        this.stepClass = stepClass;
        if(label.isPresent()){
            this.setLabel(label.get());
        }
        this.setFunction(traverser -> getVertexIterator(traverser));
        this.edgeLabels = edgeLabels;
    }

    private Iterator<Vertex> getVertexIterator(Iterator<E> elementIterator) {
        if(stepClass.isAssignableFrom(Vertex.class)){
            return getVerticesFromVertex(elementIterator);
        }
        else {
            return getVerticesFromEdges(elementIterator);
        }

    }

    private Iterator<Vertex> getVerticesFromVertex(Iterator<E> elementIterator) {
        List<String> originalVertexIterator = new ArrayList<>();
        elementIterator.forEachRemaining(vertex -> {
            this.addId(vertex.id());
            originalVertexIterator.add(vertex.id().toString());
        });
        //perdicates belongs to vertices query
        List<HasContainer> predicates = this.getPredicates();
        this.clearPredicates();
        List<Object> vertexIds = new ArrayList<Object>();
        Map<String,List<String>> idToResultsIds = new HashMap<String,List<String>>();
        if(this.direction == Direction.BOTH){
            searchEdgesAndAddIds(Direction.IN,vertexIds,idToResultsIds);
            idToResultsIds =  searchEdgesAndAddIds(Direction.OUT,vertexIds,idToResultsIds);
        }
        else {
            idToResultsIds =  searchEdgesAndAddIds(this.direction,vertexIds,idToResultsIds);
        }

        if(vertexIds.isEmpty()) return (new ArrayList<Vertex>()).iterator();
        //remove ids from edges query and put new ones
        this.clearIds();
        this.addIds(vertexIds.toArray());
        this.addPredicates(predicates);

        FilterBuilder filter = FilterBuilderProvider.getFilter(this);
        Object[] ids = this.getIds();
        //Iterator<Vertex> vertexIterator = searchWithDups(Vertex.class, null);


        Iterator<Vertex> vertexIterator = this.elasticService.searchVertices(filter);
        return addJumpingPointsAndReturnCorrectedIterator(originalVertexIterator,idToResultsIds,vertexIterator);
    }

    private Map<String,List<String>> searchEdgesAndAddIds(Direction direction, List<Object> vertexIds, Map<String,List<String>> vertexToResultVertices) {
        //Iterator<Edge> edgeIterator = searchWithDups(Edge.class, direction, edgeLabels);
        Iterator<Edge> edgeIterator = this.elasticService.searchEdges(FilterBuilderProvider.getFilter(this,direction),this.edgeLabels );
        edgeIterator.forEachRemaining(edge -> {
            Object idOposite = ((ElasticEdge) edge).getVertexId(direction.opposite()).get(0);
            String idOpositeString = idOposite.toString();
            String idNormal = ((ElasticEdge) edge).getVertexId(direction).get(0).toString();
            vertexIds.add(idOposite);
            List<String> cacheList;
            if(!vertexToResultVertices.containsKey(idNormal)){
                cacheList = new ArrayList<String>();
                vertexToResultVertices.put(idNormal,cacheList);
            }
            else cacheList = vertexToResultVertices.get(idNormal);
            cacheList.add(idOpositeString);
        });
        return vertexToResultVertices;
    }

    private Iterator<Vertex> getVerticesFromEdges(Iterator<E> elementIterator) {
        Map<String,List<String>> edgeIdToResultsIds =  new HashMap<String,List<String>>();
        List<String> originalEdgeIds = new ArrayList<String>();
        while(elementIterator.hasNext()){
            ElasticEdge edge = (ElasticEdge) elementIterator.next();
            List vertexIds = edge.getVertexId(direction);
            this.addIds(vertexIds.toArray());
            String edgeId = edge.id().toString();
            edgeIdToResultsIds.put(edgeId, vertexIds);
            originalEdgeIds.add(edgeId);
        }

        //String label = this.getLabel().isPresent()?  this.label.get() : null;
        Iterator<Vertex> vertexIterator = this.elasticService.searchVertices(FilterBuilderProvider.getFilter(this));
        return addJumpingPointsAndReturnCorrectedIterator(originalEdgeIds,edgeIdToResultsIds,vertexIterator);
    }



    @Override
    public String toString(){
        if(this.edgeLabels!=null && this.edgeLabels.length > 0)
            return TraversalHelper.makeStepString(this, this.direction, this.stepClass.getSimpleName().toLowerCase(), Arrays.asList(this.edgeLabels),this.getPredicates());
        return TraversalHelper.makeStepString(this, this.direction, this.stepClass.getSimpleName().toLowerCase(),this.getPredicates());
    }



    private Iterator<Vertex> addJumpingPointsAndReturnCorrectedIterator(List<String> inputIds,Map<String,List<String>> inputIdToResultSet,Iterator<Vertex> resultSetFromSearch){
        HashMap<String,Vertex> idToVertex  = new HashMap<String,Vertex>();
        while(resultSetFromSearch.hasNext()){
            Vertex v = resultSetFromSearch.next();
            idToVertex.put(v.id().toString(),v);
        }
        List<Vertex> vertices = new ArrayList<Vertex>();
        int counter = 0 ;
        for(String fromVertexId : inputIds){
            if(inputIdToResultSet.containsKey(fromVertexId)) {
                List<String> toVertices = inputIdToResultSet.get(fromVertexId);
                for (String vertexId : toVertices) {
                    if(idToVertex.containsKey(vertexId)) {
                        vertices.add(idToVertex.get(vertexId));
                        counter++;
                    }
                }
            }
            this.jumpingPoints.add(counter);
        }
        return vertices.iterator();

    }

}
