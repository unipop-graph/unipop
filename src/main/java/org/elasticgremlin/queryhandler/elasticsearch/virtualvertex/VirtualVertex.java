package org.elasticgremlin.queryhandler.elasticsearch.virtualvertex;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.elasticgremlin.structure.BaseVertex;
import org.elasticgremlin.structure.BaseVertexProperty;
import org.elasticgremlin.structure.ElasticGraph;

public class VirtualVertex extends BaseVertex {
    protected VirtualVertex(Object id, String label, ElasticGraph graph, Object[] keyValues) {
        super(id, label, graph, keyValues);
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {

    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {

    }
}
