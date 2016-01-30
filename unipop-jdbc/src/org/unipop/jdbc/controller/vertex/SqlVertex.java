package org.unipop.jdbc.controller.vertex;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.unipop.jdbc.helpers.SqlLazyGetter;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.sql;
import static org.jooq.impl.DSL.table;

public class SqlVertex extends BaseVertex<SqlVertexController>{

    private SqlLazyGetter sqlLazyGetter;

    protected SqlVertex(Object id, String label, Map<String, Object> keyValues, SqlLazyGetter lazyGetter, String tableName, SqlVertexController controller, UniGraph graph) {
        super(id, label, keyValues, controller, graph);
        this.sqlLazyGetter = lazyGetter;
        if (sqlLazyGetter != null)
            sqlLazyGetter.register(this, label, tableName);
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
        getController().getContext().update(table(label))
                .set(field(property.key()), n)
                .where(field("ID").equal(this.id()))
                .execute();
    }

    @Override
    protected void innerRemove() {
        getController().getContext().delete(table(label))
                .where(field("ID").equal(this.id()))
                .execute();
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        checkLazy();
        return super.property(key);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        checkLazy();
        return super.properties(propertyKeys);
    }

    protected void checkLazy() {
        if (sqlLazyGetter != null) sqlLazyGetter.execute();
    }
}
