package org.unipop.elastic.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertexController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by sbarzilay on 2/10/16.
 */
public class AggsControllerManager implements ControllerManager{

    private EdgeController edgeController;
    private VertexController vertexController;
    private VertexController aggs;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        String indexName = "modern";

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        edgeController = new ElasticEdgeController(graph, client, elasticMutations, indexName, 0, timing);
        vertexController = new ElasticVertexController(graph, client, elasticMutations, indexName, 0, timing);
        aggs = new TemplateVertexController(graph, client, elasticMutations, 0, timing, indexName, "dyn_template", ScriptService.ScriptType.FILE);
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

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        return Stream.of(vertexController, aggs).flatMap(controller -> {
            Iterable<BaseVertex> iter = () -> controller.vertices(predicates);
            return StreamSupport.stream(iter.spliterator(), false);
        }).collect(Collectors.toList()).iterator();
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return Stream.of(vertexController, aggs).map(controller -> controller.vertex(direction, vertexId, vertexLabel)).findFirst().get();
    }

    @Override
    public long vertexCount(Predicates predicates) {
        return 0;
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        return null;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {

    }

    @Override
    public void close() {
        client.close();
    }
}
