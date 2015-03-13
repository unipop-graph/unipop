package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.Traversal;
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
        elementIterator.forEachRemaining(vertex -> this.addId(vertex.id()));
        //perdicates belongs to vertices query
        List<HasContainer> predicates = this.getPredicates();
        this.clearPredicates();
        List<Object> vertexIds = new ArrayList<Object>();
        if(this.direction == Direction.BOTH){
            searchEdgesAndAddIds(Direction.IN,vertexIds);
            searchEdgesAndAddIds(Direction.OUT,vertexIds);
        }
        else {
            searchEdgesAndAddIds(this.direction,vertexIds);
        }

        if(vertexIds.isEmpty()) return (new ArrayList<Vertex>()).iterator();
        //remove ids from edges query and put new ones
        this.clearIds();
        this.addIds(vertexIds.toArray());
        this.addPredicates(predicates);

        return searchWithDups(Vertex.class,null);

    }

    private void searchEdgesAndAddIds(Direction direction, List<Object> vertexIds) {
        Iterator<Edge> edgeIterator = searchWithDups(Edge.class, direction, edgeLabels);
        edgeIterator.forEachRemaining(edge -> vertexIds.addAll(((ElasticEdge) edge).getVertexId(direction.opposite())));
    }

    private Iterator<Vertex> getVerticesFromEdges(Iterator<E> elementIterator) {
        while(elementIterator.hasNext()){
            ElasticEdge edge = (ElasticEdge) elementIterator.next();
            this.addIds(edge.getVertexId(direction).toArray());
        }
        String label = this.getLabel().isPresent()?  this.label.get() : null;
        return searchWithDups(Vertex.class,null,label);
    }



    @Override
    public String toString(){
        if(this.edgeLabels!=null && this.edgeLabels.length > 0)
            return TraversalHelper.makeStepString(this, this.direction, this.stepClass.getSimpleName().toLowerCase(), Arrays.asList(this.edgeLabels),this.getPredicates());
        return TraversalHelper.makeStepString(this, this.direction, this.stepClass.getSimpleName().toLowerCase(),this.getPredicates());
    }
}
