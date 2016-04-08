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
import org.unipop.query.UniQuery;
import org.unipop.query.controller.ControllerProvider;
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

    Map<String, VertexQueryController> vertexControllers;
    EdgeQueryController edgeQueryController;
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
        VertexQueryController movie = new AggregationsVertexController(graph, client, elasticMutations, 0, timing, indexName, "aggs_test", ScriptService.ScriptType.FILE, paths);
        vertexControllers.put("person", movie);
        vertexControllers.put("ages", movie);

        edgeQueryController = ((EdgeQueryController) movie);

    }

    @Override
    public List<UniElement> properties(List<UniElement> elements) {
        throw new NotImplementedException();
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public Iterator<UniEdge> edges(UniQuery uniQuery) {
        return edgeQueryController.edges(uniQuery);
    }

    @Override
    public Iterator<UniEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        return edgeQueryController.edges(vertices, direction, edgeLabels, uniQuery);
    }

    @Override
    public long edgeCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public UniEdge addEdge(Object edgeId, String label, UniVertex outV, UniVertex inV, Map<String, Object> properties) {
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
    public Iterator<UniVertex> vertices(UniQuery uniQuery) {
        String label = getLabel(uniQuery.hasContainers);
        if (label == null)
            return vertexControllers.get("person").vertices(uniQuery);
        return vertexControllers.get(label).vertices(uniQuery);
    }

    @Override
    public UniVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return vertexControllers.get(vertexLabel).vertex(direction, vertexId, vertexLabel);
    }

    @Override
    public long vertexCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public UniVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return null;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void addPropertyToVertex(UniVertex vertex, UniVertexProperty vertexProperty) {
        throw new NotImplementedException();
    }

    @Override
    public void removePropertyFromVertex(UniVertex vertex, Property property) {
        throw new NotImplementedException();
    }

    @Override
    public void removeVertex(UniVertex vertex) {
        throw new NotImplementedException();
    }

    @Override
    public List<UniElement> vertexProperties(List<UniVertex> vertices) {
        throw new NotImplementedException();
    }

    @Override
    public void update(UniVertex vertex, boolean force) {
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
