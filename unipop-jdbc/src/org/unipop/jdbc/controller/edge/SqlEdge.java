package org.unipop.jdbc.controller.edge;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * Created by sbarzilay on 28/01/16.
 */
public class SqlEdge extends BaseEdge {

    public SqlEdge(Object id, String label, Map<String, Object> keyValues, Vertex outV, Vertex inV, SqlEdgeController controller, UniGraph graph) {
        super(id, label, keyValues, outV, inV, controller, graph);
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        ((SqlEdgeController) getController()).getContext().update(table(label))
                .set(field(vertexProperty.key()), vertexProperty.value())
                .where(field("ID").eq(this.id()))
                .execute();
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        Object n = null;
        ((SqlEdgeController) getController()).getContext().update(table(label))
                .set(field(property.key()), n)
                .where(field("ID").equal(this.id()))
                .execute();
    }

    @Override
    protected void innerRemove() {
        ((SqlEdgeController) getController()).getContext().delete(table(label))
                .where(field("ID").equal(this.id()))
                .execute();
    }
}
