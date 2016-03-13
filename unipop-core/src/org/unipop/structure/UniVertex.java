package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.unipop.controllerprovider.ControllerManager;

import java.util.*;

/**
 * Created by sbarzilay on 3/9/16.
 */
public class UniVertex extends BaseVertex {
    Map<String, TransientProperty> transientProperties;
    public UniVertex(Object id, String label, Map<String, Object> keyValues, ControllerManager manager, UniGraph graph) {
        super(id, label, keyValues, manager, graph);
        transientProperties = new HashMap<>();
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        getManager().addPropertyToVertex(this, vertexProperty);
    }

    public void addTransientProperty(TransientProperty transientProperty){
        transientProperties.put(transientProperty.key(), transientProperty);
    }

    public boolean hasProperty(){
        return !properties.isEmpty();
    }

    public Map<String, TransientProperty> getTransientProperties(){
        return transientProperties;
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        getManager().removePropertyFromVertex(this, property);
    }

    @Override
    protected void innerRemove() {
        getManager().removeVertex(this);
    }
}
