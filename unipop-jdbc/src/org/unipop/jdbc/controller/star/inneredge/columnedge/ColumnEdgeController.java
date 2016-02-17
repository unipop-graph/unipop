package org.unipop.jdbc.controller.star.inneredge.columnedge;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.util.xml.jaxb.Table;
import org.unipop.jdbc.controller.star.SqlStarVertex;
import org.unipop.jdbc.controller.star.inneredge.InnerEdge;
import org.unipop.jdbc.controller.star.inneredge.InnerEdgeController;
import org.unipop.structure.BaseVertex;
import scala.tools.cmd.gen.AnyVals;

import java.sql.Connection;
import java.util.*;

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

    public ColumnEdgeController(){}

    public ColumnEdgeController(String vertexLabel, String edgeLabel, String externalVertexLabel, Direction direction, Connection connection, String... edgeProperties) {
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.context = DSL.using(connection, SQLDialect.SQLITE);
        this.edgeProperties = new HashSet<>();
        Collections.addAll(this.edgeProperties, edgeProperties);
    }

    @Override
    public InnerEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        SqlStarVertex starVertex = (SqlStarVertex) (direction.equals(Direction.OUT) ? outV : inV);
        BaseVertex externalVertex = direction.equals(Direction.OUT) ? inV : outV;
        ColumnEdge columnEdge = new ColumnEdge(starVertex, edgeId, edgeLabel, outV, inV, this, edgeProperties);
        starVertex.addInnerEdge(columnEdge);
        String tableName = starVertex.getController().getTableName();
        List<Field> fields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        starVertex.allFields().forEach((key, value) -> {
            fields.add(field(key));
            values.add(value);
        });
        fields.add(field(edgeLabel));
        values.add(Integer.parseInt(externalVertex.id().toString()));
        properties.forEach((key, value) ->{
            fields.add(field(key));
            values.add(value);
        });
        if (edgeId != null){
            fields.add(field("edgeid"));
            values.add(edgeId);
        }
        fields.add(field("id"));
        values.add(starVertex.id());
        Field[] fieldsArray = fields.toArray(new Field[fields.size()]);
        Object[] valuesArray = values.toArray();
        context.insertInto(table(tableName), fieldsArray).values(valuesArray).execute();
        return columnEdge;
    }

    @Override
    public InnerEdge parseEdge(SqlStarVertex vertex, Map<String, Object> keyValues) {
        BaseVertex externalVertex = vertex.getGraph().getControllerManager().vertex(direction.opposite(), keyValues.get(edgeLabel), externalVertexLabel);
        BaseVertex inV = direction.equals(Direction.IN) ? vertex : externalVertex;
        BaseVertex outV = direction.equals(Direction.OUT) ? vertex : externalVertex;
        ColumnEdge columnEdge;
        if (!keyValues.containsKey("edgeid")){
            columnEdge = new ColumnEdge(vertex, vertex.id() + edgeLabel + externalVertex.id(), edgeLabel, outV, inV, this, edgeProperties);
        }
        else{
            columnEdge = new ColumnEdge(vertex, keyValues.get("edgeid"), edgeLabel, outV, inV, this, edgeProperties);
        }
        edgeProperties.forEach(prop -> columnEdge.addPropertyLocal(prop, keyValues.get(prop)));
        vertex.addInnerEdge(columnEdge);
        return columnEdge;
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
    public void init(Map<String, Object> conf) throws Exception {
        this.edgeLabel = conf.get("edgeLabel").toString();
        this.externalVertexLabel = conf.get("externalVertexLabel").toString();
        this.direction = conf.getOrDefault("direction", "out").toString().toLowerCase().equals("out") ? Direction.OUT : Direction.IN;
    }
}
