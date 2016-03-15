package org.unipop.elastic2.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.provider.ControllerProvider;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic2.controller.aggregations.controller.vertex.AggregationsVertexController;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.elastic2.helpers.ElasticHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.*;

import java.util.*;

/**
 * Created by sbarzilay on 2/12/16.
 */
public class ImdbControllerManager implements ControllerProvider {

    Map<String, VertexController> vertexControllers;
    EdgeController edgeController;
    private Client client;
    private TimingAccessor timing;
    private ElasticMutations elasticMutations;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        client = ElasticClientFactory.create(configuration);
        String indexName = "modern";
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);

        vertexControllers = new HashMap<>();
        Map<String,String> paths = new HashMap<>();
        paths.put("hits.hits", "");
        paths.put("aggregations.ages.buckets", "ages");
        VertexController movie = new AggregationsVertexController(graph, client, elasticMutations, 0, timing, indexName, "aggs_test", ScriptService.ScriptType.FILE, paths);
        vertexControllers.put("person", movie);
        vertexControllers.put("ages", movie);

        edgeController = ((EdgeController) movie);

    }

    @Override
    public List<BaseElement> properties(List<BaseElement> elements) {
        throw new NotImplementedException();
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        return edgeController.edges(predicates);
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return edgeController.edges(vertices, direction, edgeLabels, predicates);
    }

    @Override
    public long edgeCount(Predicates predicates) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return null;
    }

    private String getLabel(ArrayList<HasContainer> hasContainers){
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getKey().equals("~label"))
                return hasContainer.getValue().toString();
        }
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        String label = getLabel(predicates.hasContainers);
        if (label == null)
            return vertexControllers.get("person").vertices(predicates);
        return vertexControllers.get(label).vertices(predicates);
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return vertexControllers.get(vertexLabel).vertex(direction, vertexId, vertexLabel);
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return null;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void addPropertyToVertex(BaseVertex vertex, BaseVertexProperty vertexProperty) {
        throw new NotImplementedException();
    }

    @Override
    public void removePropertyFromVertex(BaseVertex vertex, Property property) {
        throw new NotImplementedException();
    }

    @Override
    public void removeVertex(BaseVertex vertex) {
        throw new NotImplementedException();
    }

    @Override
    public List<BaseElement> vertexProperties(List<BaseVertex> vertices) {
        throw new NotImplementedException();
    }

    @Override
    public void update(BaseVertex vertex, boolean force) {
        throw new NotImplementedException();
    }

    @Override
    public String getResource() {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        client.close();
    }
}
