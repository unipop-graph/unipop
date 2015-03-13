package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
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
    ElasticService elasticService;
    Direction direction;
    List<String> edgeLabels;
    public EdgeSearchStep(Traversal traversal,Direction direction,ElasticService elasticService, Optional<String> label,String... edgeLabels) {
        super(traversal);
        this.elasticService = elasticService;
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
        vertexIterator.forEachRemaining(vertex -> this.addId(vertex.id()));
        Iterator<Edge> edgeIterator = this.elasticService.searchEdges(FilterBuilderProvider.getFilter(this, direction),edgeLabels.toArray(new String[edgeLabels.size()]));
        return edgeIterator;
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
