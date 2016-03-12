package org.unipop.elastic2.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.elastic2.controller.star.ElasticStarController;
import org.unipop.elastic2.controller.star.inneredge.nested.NestedEdgeController;
import org.unipop.elastic2.helpers.ElasticClientFactory;
import org.unipop.elastic2.helpers.ElasticHelper;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticStarControllerManager extends BasicControllerManager {
    private ElasticStarController controller;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;
    private String indexName;

    @Override
    protected VertexController getDefaultVertexController() {
        return controller;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return controller;
    }

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        this.indexName = configuration.getString("graphName", "unipop");

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        controller = new ElasticStarController(graph, client, elasticMutations ,indexName, 0, timing);
    }


    @Override
    public List<BaseElement> properties(List<BaseElement> elements) {
        List<BaseVertex> vertices = elements.stream().filter(element -> element instanceof UniDelayedStarVertex)
                .map(element -> ((BaseVertex) element)).collect(Collectors.toList());

        return vertexProperties(vertices);
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        ElasticHelper.mapNested(client, indexName, outV.label(), label);
        HashMap<String, Object> transientProperties = new HashMap<>();
        transientProperties.put("resource","standard");
        controller.addEdgeMapping(new NestedEdgeController(outV.label(), label, Direction.OUT, "vertex_id", inV.label(), "edge_id", transientProperties));
        return super.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public void addPropertyToVertex(BaseVertex vertex, BaseVertexProperty vertexProperty) {
        controller.addPropertyToVertex(vertex, vertexProperty);
    }

    @Override
    public void removePropertyFromVertex(BaseVertex vertex, Property property) {
        controller.removePropertyFromVertex(vertex, property);
    }

    @Override
    public void removeVertex(BaseVertex vertex) {
        controller.removeVertex(vertex);
    }

    @Override
    public List<BaseElement> vertexProperties(List<BaseVertex> vertices) {
        List<BaseElement> finalElements = new ArrayList<>();
        Map<Object, List<BaseVertex>> resource = vertices.stream().collect(Collectors.groupingBy(vertex -> ((UniVertex) vertex).getTransientProperties().get("resource").value()));
        resource.forEach((key, value) -> {
            if (controller.getResource().equals(key))
                controller.vertexProperties(value).forEach(finalElements::add);
        });
        return finalElements;
    }

    @Override
    public void update(BaseVertex vertex, boolean force) {
        controller.update(vertex, force);
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
