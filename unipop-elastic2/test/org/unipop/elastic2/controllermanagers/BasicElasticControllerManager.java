package org.unipop.elastic2.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controller.standard.BasicControllerManager;
import org.unipop.elastic2.controller.edge.ElasticEdgeController;
import org.unipop.elastic2.controller.vertex.ElasticVertexController;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.elastic2.helpers.ElasticHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.*;

import java.util.List;
import java.util.stream.Collectors;

public class BasicElasticControllerManager extends BasicControllerManager {

    private EdgeController edgeController;
    private VertexController vertexController;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = configuration.getString("graphName", "unipop");

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 0, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 0, timing);
    }

    @Override
    public List<BaseElement> properties(List<BaseElement> elements) {
        List<BaseVertex> vertices = elements.stream().filter(element -> element instanceof UniDelayedVertex)
                .map(element -> ((BaseVertex) element)).collect(Collectors.toList());

        return vertexProperties(vertices);
    }

    @Override
    protected VertexController getDefaultVertexController() {
        return vertexController;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return edgeController;
    }

    @Override
    public void commit() { elasticMutations.commit(); }

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
        return vertexController.vertexProperties(vertices);
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
        timing.print();
    }
}
