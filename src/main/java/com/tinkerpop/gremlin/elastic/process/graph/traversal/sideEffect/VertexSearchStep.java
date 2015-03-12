package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by Eliran on 11/3/2015.
 */
public class VertexSearchStep<E extends Element> extends ElasticSearchFlatMap<E,Vertex> {

    ElasticService elasticService;
    Direction direction;
    Class<E> stepClass;
    String[] edgeLabels;
    public VertexSearchStep(Traversal traversal, Direction direction, ElasticService elasticService,Class<E> stepClass, Optional<String> label,String... edgeLabels) {
        super(traversal);
        this.elasticService = elasticService;
        this.direction = direction;
        this.stepClass = stepClass;
        if(label.isPresent()){
            this.setLabel(label.get());
        }
        this.setFunction(traverser -> geVertexIterator(traverser));
        this.edgeLabels = edgeLabels;
    }

    private Iterator<Vertex> geVertexIterator(Iterator<E> elementIterator) {
        if(stepClass.isAssignableFrom(Vertex.class)){
            return getVertexexFromVertex(elementIterator);
        }
        else {
            return getVertexesFromEdges(elementIterator);
        }

    }

    private Iterator<Vertex> getVertexexFromVertex(Iterator<E> elementIterator) {
        elementIterator.forEachRemaining(vertex -> this.addId(vertex.id()));
        Iterator<Edge> edgeIterator = this.elasticService.searchEdges(FilterBuilderProvider.getFilter(this, this.direction),edgeLabels);
        List<String> edgesIds = new ArrayList<String>();
        while(edgeIterator.hasNext()){
            ElasticEdge edge = (ElasticEdge) edgeIterator.next();
            List<Object> objects = edge.getVertexId(this.direction.opposite());
            objects.iterator().forEachRemaining(idFromEdge -> edgesIds.add(idFromEdge.toString()));
        }
        BoolFilterBuilder filterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.idsFilter().addIds(edgesIds.toArray(new String[edgesIds.size()])));
        return this.elasticService.searchVertices(filterBuilder);

    }

    private Iterator<Vertex> getVertexesFromEdges(Iterator<E> elementIterator) {
        while(elementIterator.hasNext()){
            ElasticEdge edge = (ElasticEdge) elementIterator.next();
            this.addIds(edge.getVertexId(direction).toArray());
        }
        return elasticService.searchVertices(FilterBuilderProvider.getFilter(this),label.get());
    }
}
