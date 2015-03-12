package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Optional;

/**
 * Created by Eliran on 11/3/2015.
 */
public class EdgeSearchStep extends ElasticSearchFlatMap<Vertex,Edge> {
    ElasticService elasticService;
    Direction direction;
    String[] edgeLabels;
    public EdgeSearchStep(Traversal traversal,Direction direction,ElasticService elasticService, Optional<String> label,String... edgeLabels) {
        super(traversal);
        this.elasticService = elasticService;
        this.direction = direction;
        this.setFunction(traverser ->
                getEdgesIterator(traverser) );
        if(label.isPresent()){
            this.setLabel(label.get());
        }
        this.edgeLabels = edgeLabels;
    }

    private Iterator<Edge> getEdgesIterator(Iterator<Vertex> vertexIterator) {
        vertexIterator.forEachRemaining(vertex -> this.addId(vertex.id()));
        Iterator<Edge> edgeIterator = this.elasticService.searchEdges(FilterBuilderProvider.getFilter(this, direction),edgeLabels);
        return edgeIterator;
    }
}
