package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.controller.EdgeController;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.structure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class InnerEdge extends BaseEdge {

    private final EdgeMapping mapping;
    private final ElasticMutations mutations;
    private final String index;

    public InnerEdge(Object edgeId, EdgeMapping mapping, Vertex outVertex, Vertex inVertex, Map<String, Object> keyValues, EdgeController controller, UniGraph graph, ElasticMutations mutations, String index) {
        super(edgeId, mapping.getLabel(), keyValues, outVertex, inVertex, controller, graph);
        this.mapping = mapping;
        this.mutations = mutations;
        this.index = index;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    private ElasticStarVertex getVertexContainer() {
        return (mapping.getDirection().equals(Direction.OUT)) ? (ElasticStarVertex) outVertex : (ElasticStarVertex) inVertex;

    }

    @Override
    protected void innerRemoveProperty(Property property) {
        properties.remove(property.key());
        try {
            mutations.updateElement(getVertexContainer(), index, null, false);
            mutations.refresh();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerRemove() {
        try {
            getVertexContainer().removeEdge(this);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        properties.put(vertexProperty.key(), vertexProperty);
        try {
            mutations.updateElement(getVertexContainer(),index,null,false);
            mutations.refresh();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public EdgeMapping getMapping() {
        return mapping;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Property p = super.property(key, value);
        try {
            mutations.updateElement(outVertex, ((ElasticStarVertex) outVertex).indexName, null, false);
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
