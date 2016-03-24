package org.unipop.integration.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.manager.ControllerProvider;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.jdbc.controller.star.SqlStarController;
import org.unipop.jdbc.controller.star.inneredge.columnedge.ColumnEdgeController;
import org.unipop.jdbc.controller.vertex.SqlVertexController;
import org.unipop.structure.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class IntegrationControllerManager implements ControllerProvider {
    private Map<String, EdgeController> edgeController;
    private Map<String, VertexController> vertexController;
    private Connection jdbcConnection;

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        vertexController = new HashMap<>();
        edgeController = new HashMap<>();
        Class.forName("org.sqlite.JDBC");
        this.jdbcConnection = DriverManager.getConnection("jdbc:sqlite:test.sqlite");
        Map<String, Object> transientPropertiesKnows = new HashMap<>();
        Map<String, Object> transientPropertiescreated = new HashMap<>();

        transientPropertiesKnows.put("resource", "person");
        transientPropertiescreated.put("resource", "software");

        ColumnEdgeController knows = new ColumnEdgeController("person", "knows", "person", Direction.OUT, jdbcConnection, transientPropertiesKnows, "weight");
        ColumnEdgeController created = new ColumnEdgeController("software", "created", "person", Direction.IN, jdbcConnection, transientPropertiescreated, "weight");

        SqlStarController person = new SqlStarController("PERSON", graph, jdbcConnection, new HashSet<String>() {{
            add("name");
            add("age");
        }}, knows);
        SqlVertexController animal = new SqlVertexController("animal", graph, jdbcConnection);
        SqlStarController software = new SqlStarController("SOFTWARE", graph, jdbcConnection, new HashSet<String>() {{
            add("name");
            add("lang");
        }}, created);
        SqlVertexController song = new SqlVertexController("SONG", graph, jdbcConnection);
        SqlVertexController artist = new SqlVertexController("ARTIST", graph, jdbcConnection);

        vertexController.put("person", person);
//        vertexController.put("animal", animal);
        vertexController.put("software", software);
//        vertexController.put("song", song);
//        vertexController.put("artist", artist);

        edgeController.put("knows", person);
        edgeController.put("created", software);
//        edgeController.put("followedBy", new SqlEdgeController("FOLLOWEDBY", "inid", "inlabel", "outid", "outlabel", "followedBy", graph, jdbcConnection));
//        edgeController.put("sungBy", new SqlEdgeController("SUNGBY", "inid", "inlabel", "outid", "outlabel", "sungBy", graph, jdbcConnection));
//        edgeController.put("writtenBy", new SqlEdgeController("WRITTERBY", "inid", "inlabel", "outid", "outlabel", "writtenBy", graph, jdbcConnection));
//        edgeController.put("createdBy", new SqlEdgeController("createdBy", "inid", "inlabel", "outid", "outlabel", "createdBy", graph, jdbcConnection));
//        edgeController.put("co-developer", new SqlEdgeController("codeveloper", "inid", "inlabel", "outid", "outlabel", "co-developer", graph, jdbcConnection));
//        edgeController.put("existsWith", new SqlEdgeController("existsWith", "inid", "inlabel", "outid", "outlabel", "existsWith", graph, jdbcConnection));
    }

    @Override
    public List<BaseElement> properties(List<BaseElement> elements) {
        List<BaseVertex> vertices = elements.stream().filter(element -> element instanceof UniDelayedStarVertex)
                .map(element -> ((BaseVertex) element)).collect(Collectors.toList());

        return vertexProperties(vertices);

    }

    @Override
    public void commit() {

    }

    @Override
    public void close() {
        try {
            if (jdbcConnection != null)
                jdbcConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        String label = getLabel(predicates);
        if (label == null) {
            return edgeController.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(edgeController1 ->
                            StreamSupport.stream(Spliterators.
                                            spliteratorUnknownSize(edgeController1.edges(predicates),
                                                    Spliterator.ORDERED),
                                    false)).collect(Collectors.toSet()).iterator();
        } else {
            return edgeController.get(label).edges(predicates);
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        if (edgeLabels.length == 0) {
            return edgeController.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(edgeController1 ->
                            StreamSupport.stream(Spliterators.
                                            spliteratorUnknownSize(edgeController1.
                                                            edges(vertices, direction, edgeLabels, predicates),
                                                    Spliterator.ORDERED),
                                    false)).collect(Collectors.toSet()).iterator();
        } else {
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
                                    false)).collect(Collectors.toSet()).iterator();
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

    private String getLabel(Predicates predicates) {
        for (HasContainer hasContainer : predicates.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()))
                return hasContainer.getValue().toString();
        }
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        String label = getLabel(predicates);

        if (label == null) {
            return vertexController.entrySet()
                    .stream()
                    .map(Map.Entry::getValue)
                    .flatMap(vertexController1 ->
                            StreamSupport.stream(Spliterators.
                                            spliteratorUnknownSize(vertexController1.vertices(predicates),
                                                    Spliterator.ORDERED),
                                    false)).collect(Collectors.toList()).iterator();
        } else {
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

    @Override
    public void addPropertyToVertex(BaseVertex vertex, BaseVertexProperty vertexProperty) {
        vertexController.get(vertex.label()).addPropertyToVertex(vertex, vertexProperty);
    }

    @Override
    public void removePropertyFromVertex(BaseVertex vertex, Property property) {
        vertexController.get(vertex.label()).removePropertyFromVertex(vertex, property);
    }

    @Override
    public void removeVertex(BaseVertex vertex) {
        vertexController.get(vertex.label()).removeVertex(vertex);
    }

    @Override
    public List<BaseElement> vertexProperties(List<BaseVertex> vertices) {
        Map<String, List<BaseVertex>> groupedElements = vertices.stream().map(baseElement -> baseElement).collect(Collectors.groupingBy(Element::label));
        List<BaseElement> finalElements = new ArrayList<>();
        // key = label
        groupedElements.forEach((key, value) ->{
            Map<Object, List<BaseVertex>> groupedByResorceVertices = value.stream().collect(Collectors.groupingBy(vertex -> ((UniVertex) vertex).getTransientProperties().get("resource").value()));
            groupedByResorceVertices.forEach((resource, vertexList) ->{
                if (vertexController.get(key).getResource().equals(resource))
                    vertexController.get(key).vertexProperties(value).forEach(finalElements::add);
            });
        });
        return finalElements;
    }

    @Override
    public void update(BaseVertex vertex, boolean force) {

    }

    @Override
    public String getResource() {
        throw new NotImplementedException();
    }
}
