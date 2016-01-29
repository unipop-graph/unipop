package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic2.controller.schema.SchemaEdge;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Roman on 3/21/2015.
 */
public class EdgeHelper {
    static public String getEdgeSourceId(Element element, GraphEdgeSchema edgeSchema, String defaultValue) {
        String value = defaultValue;
        if (edgeSchema.getSource().isPresent()) {
            Object elementValue = element.value(edgeSchema.getSource().get().getIdField());
            if (elementValue != null) {
                value = elementValue.toString();
            }
        }

        return value;
    }

    static public String getEdgeDestinationId(Element element, GraphEdgeSchema edgeSchema, String defaultValue) {
        String value = defaultValue;
        if (edgeSchema.getDestination().isPresent()) {
            Object elementValue = element.value(edgeSchema.getDestination().get().getIdField());
            if (elementValue != null) {
                value = elementValue.toString();
            }
        }

        return value;
    }

    static public String getEdgeSourceIdField(GraphEdgeSchema edgeSchema, String defaultValue) {
        return edgeSchema.getSource().isPresent() ? edgeSchema.getSource().get().getIdField() : defaultValue;
    }

    static public String getEdgeSourceType(GraphEdgeSchema edgeSchema, String defaultValue) {
        return edgeSchema.getSource().isPresent() && edgeSchema.getSource().get().getType().isPresent() ? edgeSchema.getSource().get().getType().get() : defaultValue;
    }

    static public String getEdgeDestinationIdField(GraphEdgeSchema edgeSchema, String defaultValue) {
        return edgeSchema.getSource().isPresent() ? edgeSchema.getDestination().get().getIdField() : defaultValue;
    }

    static public String getEdgeDestinationType(GraphEdgeSchema edgeSchema, String defaultValue) {
        return edgeSchema.getDestination().isPresent() && edgeSchema.getDestination().get().getType().isPresent() ? edgeSchema.getDestination().get().getType().get() : defaultValue;
    }

    static public Optional<Direction> getEdgeDirection(Element element, GraphEdgeSchema edgeSchema) {
        String directionValue =  edgeSchema.getDirection().isPresent() ? element.value(edgeSchema.getDirection().get().getField()) : null;
        if (directionValue == null) {
            return Optional.empty();
        }

        return directionValue.equals(edgeSchema.getDirection().get().getInValue()) ? Optional.of(Direction.IN) :
                directionValue.equals(edgeSchema.getDirection().get().getOutValue()) ? Optional.of(Direction.OUT) :
                        Optional.empty();
    }

    static public Optional<GraphEdgeSchema> getEdgeSchema(GraphElementSchemaProvider schemaProvider, Element element) {
        Optional<Iterable<GraphEdgeSchema>> schemas = schemaProvider.getEdgeSchemas(element.label());

        for (GraphEdgeSchema schema : schemas.get()) {
            if (getEdgeSourceId(element, schema, null) != null &&
                    getEdgeDestinationId(element, schema, null) != null) {
                return Optional.of(schema);
            }
        }

        return Optional.empty();
    }

    static public Edge createEdge(Element element, Vertex inVertex, Vertex outVertex, GraphElementSchemaProvider schemaProvider, ElasticMutations elasticMutations) {
        final Map<String, Object> keyValues = new HashMap();
        Optional<GraphEdgeSchema> schema = schemaProvider.getEdgeSchema(
                element.label(),
                Optional.of(outVertex.label()),
                Optional.of(inVertex.label()));

        String sourceIdField = getEdgeSourceIdField(schema.get(), null);
        String destinationIdField = getEdgeDestinationIdField(schema.get(), null);

        element.properties().forEachRemaining(property -> {
            if (property.key() != null &&
                    !property.key().equals(sourceIdField) &&
                    !property.key().equals(destinationIdField)) {
                keyValues.put(property.key(), property.value());
            }
        });

        return new SchemaEdge(
                element.id(),
                element.label(),
                keyValues,
                outVertex,
                inVertex,
                null,
                (UniGraph)element.graph(),
                schema,
                elasticMutations);
    }
}
