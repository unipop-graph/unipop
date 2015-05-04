package com.tinkerpop.gremlin.elastic.process.graph.traversal.steps.flatmap;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;

import java.util.*;

public class ElasticVertexStep<E extends Element> extends ElasticFlatMapStep<Vertex,E> {

    private final Class returnClass;
    private String[] edgeLabels;
    public ElasticVertexStep(VertexStep originalStep,  ArrayList<HasContainer> hasContainers, ElasticService elasticService,Integer resultsLimit) {
        super(originalStep.getTraversal(), originalStep.getLabel(), elasticService, hasContainers, originalStep.getDirection(),resultsLimit);
        this.edgeLabels = originalStep.getEdgeLabels();
        returnClass = originalStep.getReturnClass();
    }

    @Override
    protected void load(List<ElasticTraverser> traversers) {
        if(returnClass.isAssignableFrom(Vertex.class))
             loadVertices(traversers);
        else loadEdges(traversers, hasContainers);
    }

    private void loadEdges(List<ElasticTraverser> traversers, ArrayList<HasContainer> has) {
        ArrayList<HasContainer> edgeHasContainers = has != null ? (ArrayList<HasContainer>) has.clone() : new ArrayList<>();
        edgeHasContainers.add(new HasContainer("~label", Contains.within, edgeLabels));

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

        Iterator<Edge> edgeIterator = elasticService.searchEdges(edgeHasContainers, resultsLimit, direction,
                vertexIdToTraverser.keySet().toArray());

        edgeIterator.forEachRemaining(edge -> ((ElasticEdge) edge).getVertexId(direction).forEach(vertexKey -> {
            if (vertexIdToTraverser.get(vertexKey) != null)
                vertexIdToTraverser.get(vertexKey).forEach(traverser -> traverser.addResult((E) edge));
        }));
    }

    private void loadVertices(List<ElasticTraverser> traversers) {

        loadEdges(traversers, null);

        Map<String,List<ElasticTraverser>> vertexIdToTraversers = new HashMap<>();
        traversers.forEach(traverser -> {
            traverser.getResults().forEach(edge -> {
                ElasticEdge elasticEdge = (ElasticEdge) edge;
                elasticEdge.getVertexId(direction.opposite()).forEach(id -> {
                    if (id.toString().equals(traverser.getElement().id().toString()) && //disregard self in Direction.BOTH traversals
                            direction.equals(Direction.BOTH) &&
                            !elasticEdge.getVertexId(Direction.IN).get(0).toString().equals(elasticEdge.getVertexId(Direction.OUT).get(0).toString())) // except when the vertex points to itself
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

        ArrayList<HasContainer> hasList = hasContainers;
        Object[] ids = vertexIdToTraversers.keySet().toArray();
        if(ids.length > 0) {
            hasList = (ArrayList<HasContainer>) hasContainers.clone();
            hasList.add(new HasContainer("~id", Contains.within, ids));
        }

        Iterator<Vertex> vertexIterator = elasticService.searchVertices(hasList,resultsLimit);

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
