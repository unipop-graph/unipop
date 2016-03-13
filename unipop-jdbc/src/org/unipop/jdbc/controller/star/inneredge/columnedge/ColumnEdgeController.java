package org.unipop.jdbc.controller.star.inneredge.columnedge;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.unipop.controller.InnerEdgeController;
import org.unipop.controller.Predicates;
import org.unipop.structure.*;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * Created by sbarzilay on 2/17/16.
 */
public class ColumnEdgeController implements InnerEdgeController {
    private String edgeLabel;
    private String externalVertexLabel;
    private Direction direction;
    private Set<String> edgeProperties;
    private DSLContext context;
    protected Map<String, Object> transientProperties;

    public ColumnEdgeController() {
    }

    public ColumnEdgeController(String vertexLabel, String edgeLabel, String externalVertexLabel, Direction direction, Connection connection, Map<String, Object> transientProperties, String... edgeProperties) {
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.context = DSL.using(connection, SQLDialect.SQLITE);
        this.edgeProperties = new HashSet<>();
        this.transientProperties = transientProperties;
        Collections.addAll(this.edgeProperties, edgeProperties);
    }

    @Override
    public UniInnerEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        UniStarVertex starVertex = (UniStarVertex) (direction.equals(Direction.OUT) ? outV : inV);
        BaseVertex externalVertex = direction.equals(Direction.OUT) ? inV : outV;
        UniInnerEdge columnEdge = new UniInnerEdge(starVertex, edgeId, edgeLabel, this, outV, inV);
        starVertex.addInnerEdge(columnEdge);
        String tableName = starVertex.label();
        List<Field> fields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        starVertex.allFields().forEach((key, value) -> {
            if (value instanceof Map[]) {
                for (Map<String, Object> map : ((Map<String, Object>[]) value))
                    map.forEach((key1, value1) -> {
                        fields.add(field(key1));
                        values.add(value1);
                    });
            } else {
                fields.add(field(key));
                values.add(value);
            }
        });
        properties.forEach((key, value) -> {
            fields.add(field(key));
            values.add(value);
        });
        fields.add(field("id"));
        values.add(starVertex.id());
        Field[] fieldsArray = fields.toArray(new Field[fields.size()]);
        Object[] valuesArray = values.toArray();
        context.insertInto(table(tableName), fieldsArray).values(valuesArray).execute();
        return columnEdge;
    }

    @Override
    public Set<UniInnerEdge> parseEdges(UniStarVertex vertex, Map<String, Object> keyValues) {
        return null;
    }

    @Override
    public UniInnerEdge parseEdge(UniStarVertex vertex, Map<String, Object> keyValues) {
        UniVertex externalVertex = (UniVertex) vertex.getGraph().getControllerManager().vertex(direction.opposite(), keyValues.get(edgeLabel), externalVertexLabel);
        transientProperties.forEach((key,value) -> externalVertex.addTransientProperty(new TransientProperty(externalVertex, key, value)));
        BaseVertex inV = direction.equals(Direction.IN) ? vertex : externalVertex;
        BaseVertex outV = direction.equals(Direction.OUT) ? vertex : externalVertex;
        UniInnerEdge columnEdge;
        if (!keyValues.containsKey("edgeid")) {
            columnEdge = new UniInnerEdge(vertex, vertex.id() + edgeLabel + externalVertex.id(), edgeLabel, this, outV, inV);
        } else {
            columnEdge = new UniInnerEdge(vertex, keyValues.get("edgeid"), edgeLabel, this, outV, inV);
        }
        edgeProperties.forEach(prop -> columnEdge.addPropertyLocal(prop, keyValues.get(prop)));
        vertex.addInnerEdge(columnEdge);
        return columnEdge;
    }

    @Override
    public Object getFilter(ArrayList<HasContainer> hasContainers) {
        return null;
    }

    @Override
    public Object getFilter(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return null;
    }

    @Override
    public Map<String, Object> allFields(List<UniInnerEdge> edges) {
        Map<String, Object>[] edgesMap = new Map[edges.size()];
        for (int i = 0; i < edges.size(); i++) {
            UniInnerEdge innerEdge = edges.get(i);
            Map<String, Object> fields = innerEdge.allFields();
            fields.put(edgeLabel, innerEdge.vertices(direction.opposite()).next().id());
            fields.put("edgeid", innerEdge.id());
            edgesMap[i] = fields;
        }
        return Collections.singletonMap(edgeLabel, edgesMap);
    }

    @Override
    public String getEdgeLabel() {
        return edgeLabel;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean shouldAddProperty(String key) {
        return !edgeProperties.contains(key) && key.equals(edgeLabel);
    }

    @Override
    public void init(Map<String, Object> conf) throws Exception {
        this.edgeLabel = conf.get("edgeLabel").toString();
        this.externalVertexLabel = conf.get("externalVertexLabel").toString();
        this.direction = conf.getOrDefault("direction", "out").toString().toLowerCase().equals("out") ? Direction.OUT : Direction.IN;
        this.context = ((DSLContext) conf.get("context"));
        this.edgeProperties = ((ArrayList<String>) conf.get("properties")).stream().collect(Collectors.toSet());
    }
}
