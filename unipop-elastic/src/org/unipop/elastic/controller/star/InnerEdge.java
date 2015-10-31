package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.controller.EdgeController;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class InnerEdge extends BaseEdge {

    private final EdgeMapping mapping;

    public InnerEdge(Object edgeId, EdgeMapping mapping, Vertex outVertex, Vertex inVertex, Map<String, Object> keyValues, EdgeController controller, UniGraph graph) {
        super(edgeId, mapping.getLabel(), keyValues,outVertex,inVertex,controller,graph);
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
        properties.put(vertexProperty.key(), vertexProperty);
    }

    public EdgeMapping getMapping() {
        return mapping;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Property p = super.property(key, value);
        try {
            ((StarController) getController()).getElasticMutations().updateElement(outVertex, ((ElasticStarVertex) outVertex).indexName,null,false);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return p;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        ArrayList<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            vertices.add(inVertex);
        }
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            vertices.add(outVertex);
        }
        return vertices.iterator();
    }

    public Map<String, Object> getMap() {
        HashMap<String, Object> map = new HashMap<>();
        properties.forEach((key, value) -> map.put(key, value.value()));
        map.put("label", label);
        map.put(ElasticEdge.InId, inVertex().id());
        map.put(ElasticEdge.InLabel, inVertex().label());
        map.put(ElasticEdge.OutId, outVertex().id());
        map.put(ElasticEdge.OutLabel, outVertex().label());
        return map;
    }
}
