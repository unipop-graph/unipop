package org.unipop.jdbc.controller.star;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class SqlVertex extends BaseVertex<SqlTableController>{

    protected SqlVertex(Object id, String label, Object[] keyValues, SqlTableController controller, UniGraph graph) {
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
        //getController().getContext().update(table(label)).set(field(property.key()));
    }

    @Override
    protected void innerRemove() {

    }
}
