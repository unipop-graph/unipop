package org.unipop.integration.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.controllerprovider.TinkerGraphControllerManager;
import org.unipop.elastic.controller.edge.ElasticEdgeController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.jdbc.controller.edge.SqlEdgeController;
import org.unipop.jdbc.controller.vertex.SqlVertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class IntegrationControllerManager implements ControllerManager {
    private Map<String, EdgeController> edgeController;
    private Map<String, VertexController> vertexController;
    private Connection jdbcConnection;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        vertexController = new HashMap<>();
        edgeController = new HashMap<>();
        Class.forName("org.sqlite.JDBC");
        this.jdbcConnection = DriverManager.getConnection("jdbc:sqlite:test.sqlite");
        vertexController.put("person", new SqlVertexController("PERSON", graph, jdbcConnection));
        vertexController.put("animal", new SqlVertexController("animal", graph, jdbcConnection));
        vertexController.put("software", new SqlVertexController("SOFTWARE", graph, jdbcConnection));
        vertexController.put("song", new SqlVertexController("SONG", graph, jdbcConnection));
        vertexController.put("artist", new SqlVertexController("ARTIST", graph, jdbcConnection));

        edgeController.put("knows", new SqlEdgeController("KNOWS", "inid", "inlabel", "outid", "outlabel", "knows", graph, jdbcConnection));
        edgeController.put("created", new SqlEdgeController("CREATED", "inid", "inlabel", "outid", "outlabel", "created", graph, jdbcConnection));
        edgeController.put("followedBy", new SqlEdgeController("FOLLOWEDBY", "inid", "inlabel", "outid", "outlabel", "followedBy", graph, jdbcConnection));
        edgeController.put("sungBy", new SqlEdgeController("SUNGBY", "inid", "inlabel", "outid", "outlabel", "sungBy", graph, jdbcConnection));
        edgeController.put("writtenBy", new SqlEdgeController("WRITTERBY", "inid", "inlabel", "outid", "outlabel", "writtenBy", graph, jdbcConnection));
        edgeController.put("createdBy", new SqlEdgeController("createdBy", "inid", "inlabel", "outid", "outlabel", "createdBy", graph, jdbcConnection));
        edgeController.put("co-developer", new SqlEdgeController("codeveloper", "inid", "inlabel", "outid", "outlabel", "co-developer", graph, jdbcConnection));
        edgeController.put("existsWith", new SqlEdgeController("existsWith", "inid", "inlabel", "outid", "outlabel", "existsWith", graph, jdbcConnection));
    }

    @Override
    public void commit() {

    }

    @Override
    public void close() {
        try {
            if(jdbcConnection != null)
                jdbcConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        String label = getLabel(predicates);
        if (label == null){
            return edgeController.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(edgeController1 ->
                            StreamSupport.stream(Spliterators.
                                            spliteratorUnknownSize(edgeController1.edges(predicates),
                                                    Spliterator.ORDERED),
                                    false)).collect(Collectors.toList()).iterator();
        }
        else{
            return edgeController.get(label).edges(predicates);
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        if (edgeLabels.length == 0){
            return edgeController.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(edgeController1 ->
                            StreamSupport.stream(Spliterators.
                                            spliteratorUnknownSize(edgeController1.
                                                    edges(vertices, direction, edgeLabels, predicates),
                                                    Spliterator.ORDERED),
                                    false)).collect(Collectors.toList()).iterator();
        }
        else{
            List<String> edges = Arrays.asList(edgeLabels);
            return edgeController.entrySet()
                    .stream()
                    .filter(entry -> edges.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .flatMap(edgeController1 ->
                            StreamSupport.stream(Spliterators.
                                            spliteratorUnknownSize(edgeController1.
                                                            edges(vertices, direction, edgeLabels, predicates),
                                                    Spliterator.ORDERED),
                                    false)).collect(Collectors.toList()).iterator();
        }
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
        return edgeController.get(label).addEdge(edgeId, label, outV, inV, properties);
    }

    private String getLabel(Predicates predicates){
        for (HasContainer hasContainer : predicates.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()))
                return hasContainer.getValue().toString();
        }
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        String label = getLabel(predicates);

        if (label == null){
            return vertexController.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(vertexController1 ->
                            StreamSupport.stream(Spliterators.
                                    spliteratorUnknownSize(vertexController1.vertices(predicates),
                                            Spliterator.ORDERED),
                                    false)).collect(Collectors.toList()).iterator();
        }
        else{
            return vertexController.get(label).vertices(predicates);
        }
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        return vertexController.get(vertexLabel).vertex(direction, vertexId, vertexLabel);
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
        return vertexController.get(label).addVertex(id, label, properties);
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {

    }
}
