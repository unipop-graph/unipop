package org.unipop.controller.elasticsearch.star;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class InnerEdge extends BaseEdge {

    private final EdgeMapping mapping;

    public InnerEdge(Object edgeId, EdgeMapping mapping, Vertex outVertex, Vertex inVertex, Object[] keyValues, UniGraph graph) {
        super(edgeId, mapping.getLabel(), keyValues, outVertex, inVertex, graph);
        this.mapping = mapping;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        throw new NotImplementedException();
    }

    @Override
    protected void innerRemove() {
        throw new NotImplementedException();
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        throw new NotImplementedException();
    }

    public EdgeMapping getMapping() {
        return mapping;
    }
}
