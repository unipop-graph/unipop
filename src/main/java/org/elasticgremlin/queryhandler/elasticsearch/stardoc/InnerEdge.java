package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.elasticgremlin.structure.BaseEdge;
import org.elasticgremlin.structure.BaseProperty;
import org.elasticgremlin.structure.ElasticGraph;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class InnerEdge extends BaseEdge {

    private final EdgeMapping mapping;

    public InnerEdge(EdgeMapping mapping, StarVertex holdingVertex, Vertex externalVertex, Object[] keyValues, ElasticGraph graph) {
        super(externalVertex.toString() + holdingVertex.id().toString(), mapping.getLabel(), keyValues, graph);
        this.mapping = mapping;
        inVertex = mapping.getDirection().equals(Direction.IN) ? holdingVertex : externalVertex;
        outVertex = mapping.getDirection().equals(Direction.OUT) ? holdingVertex : externalVertex;
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
