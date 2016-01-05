package org.unipop.elastic2.controller.schema;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 9/21/2015.
 */
public class SchemaEdge extends BaseEdge {
    //region Constructor
    public SchemaEdge(
            Object id,
            String label,
            Map<String, Object> keyValues,
            Vertex outVertex,
            Vertex inVertex,
            EdgeController edgeController,
            UniGraph graph,
            Optional<GraphEdgeSchema> schema,
            ElasticMutations elasticMutations
    ) {
        super(id, label, keyValues, outVertex, inVertex, edgeController, graph);
        this.schema = schema;
        this.elasticMutations = elasticMutations;
    }
    //endregion

    //region BaseEdge Implementation
    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        try {
            String writeIndex = StreamSupport.stream(schema.get().getIndices().spliterator(), false).findFirst().get();
            elasticMutations.updateElement(this, writeIndex, null, false);
        }
        catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        String writeIndex = StreamSupport.stream(schema.get().getIndices().spliterator(), false).findFirst().get();
        elasticMutations.addElement(this, writeIndex, null, false);
    }

    @Override
    protected void innerRemove() {
        String writeIndex = StreamSupport.stream(schema.get().getIndices().spliterator(), false).findFirst().get();
        elasticMutations.deleteElement(this, writeIndex, null);
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        if (!super.shouldAddProperty(key)) {
            return false;
        }

        if (schema == null) {
            return true;
        }

        return !key.equals(schema.get().getSource().get().getIdField()) &&
                !key.equals(schema.get().getDestination().get().getIdField());
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();

        if (!schema.isPresent()) {
            //TODO ???
            return null;
        } else {
            GraphEdgeSchema.End source = schema.get().getSource().get();
            GraphEdgeSchema.End dest = schema.get().getDestination().get();

            map.put(source.getIdField(), outVertex.id());
            map.put(dest.getIdField(), inVertex.id());
        }

        return map;
    }
    //endregion

    //region Fields
    protected Optional<GraphEdgeSchema> schema;
    protected ElasticMutations elasticMutations;
    //endregion
}
