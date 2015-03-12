package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
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
            return getVerticesFromVertex(elementIterator);
        }
        else {
            return getVertexesFromEdges(elementIterator);
        }

    }

    private Iterator<Vertex> getVerticesFromVertex(Iterator<E> elementIterator) {
        elementIterator.forEachRemaining(vertex -> this.addId(vertex.id()));
        //perdicates belongs to vertices query
        List<HasContainer> predicates = this.getPredicates();
        this.clearPredicates();
        Iterator<Edge> edgeIterator = this.elasticService.searchEdges(FilterBuilderProvider.getFilter(this,this.direction),edgeLabels);
        List<Object> vertexIds = new ArrayList<Object>();
        edgeIterator.forEachRemaining(edge -> vertexIds.addAll(((ElasticEdge) edge).getVertexId(this.direction.opposite())));
        //remove ids from last query and put new ones
        this.clearIds();
        this.addIds(vertexIds.toArray());
        this.addPredicates(predicates);
        return this.elasticService.searchVertices(FilterBuilderProvider.getFilter(this));

    }

    private Iterator<Vertex> getVertexesFromEdges(Iterator<E> elementIterator) {
        while(elementIterator.hasNext()){
            ElasticEdge edge = (ElasticEdge) elementIterator.next();
            this.addIds(edge.getVertexId(direction).toArray());
        }
        String label = this.getLabel().isPresent()?  this.label.get() : null;
        return elasticService.searchVertices(FilterBuilderProvider.getFilter(this),label);
    }
}
