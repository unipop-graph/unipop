package org.unipop.jdbc.controller.star;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class SqlVertex extends BaseVertex<SqlTableController>{

    protected SqlVertex(Object id, String label, Map<String, Object> keyValues, SqlTableController controller, UniGraph graph) {
        super(id, label, keyValues, controller, graph);
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        getController().getContext().update(table(label))
                .set(field(vertexProperty.key()), vertexProperty.value())
                .where(field("ID").eq(this.id()))
                .execute();
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        Object n = null;
        getController().getContext().update(table(label)).set(field(property.key()), n).execute();
    }

    @Override
    protected void innerRemove() {
        getController().getContext().delete(table(label)).where(field("ID").equal(this.id())).execute();
    }
}
