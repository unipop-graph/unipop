package org.unipop.jdbc.controller.star.inneredge.columnedge;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.DSLContext;
import org.unipop.jdbc.controller.star.SqlStarVertex;
import org.unipop.jdbc.controller.star.inneredge.InnerEdge;
import org.unipop.structure.BaseProperty;

import java.util.Set;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * Created by sbarzilay on 2/17/16.
 */
public class ColumnEdge extends InnerEdge<ColumnEdgeController> {
    SqlStarVertex starVertex;
    Set<String> propertiesNames;

    public ColumnEdge(SqlStarVertex starVertex, Object id, String label, Vertex outV, Vertex inV, ColumnEdgeController controller, Set<String> propertiesNames) {
        super(starVertex, id, label, outV, inV, controller);
        this.propertiesNames = propertiesNames;
        this.starVertex = starVertex;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Property<V> property = super.property(key, value);
        DSLContext context = starVertex.getController().getContext();
        context.update(table(starVertex.getController().getTableName())).set(field(key),value).where(field("edgeid").eq(id)).execute();
        return property;
    }

    @Override
    protected void innerAddProperty(BaseProperty property) {
        properties.put(property.key(), property);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        properties.remove(property.key());
    }

    @Override
    protected void innerRemove() {
        starVertex.removeInnerEdge(this);
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return super.shouldAddProperty(key) && propertiesNames.contains(key);
    }
}
