package org.unipop.elastic2.controller.schema.helpers;

import com.google.common.collect.FluentIterable;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;

import java.util.Arrays;
import java.util.Optional;

/**
 * Created by Roman on 3/28/2015.
 */
public class SearchBuilderHelper {
    public static <E extends Element> void applyIndices(
            SearchBuilder searchBuilder,
            GraphElementSchemaProvider schemaProvider,
            Class elementType) {

        Iterable<String> types = Vertex.class.isAssignableFrom(elementType) ?
                schemaProvider.getVertexTypes() : schemaProvider.getEdgeTypes();

        applyIndices(searchBuilder, schemaProvider, types, elementType);
    }

    public static <E extends Element> void applyIndices(
            SearchBuilder searchBuilder,
            GraphElementSchemaProvider schemaProvider,
            Iterable<String> types,
            Class elementType) {

        for(String type : types) {
            Optional<Iterable<? extends GraphElementSchema>> schemas = Vertex.class.isAssignableFrom(elementType) ?
                    Optional.of(Arrays.asList(schemaProvider.getVertexSchema(type).get())) :
                    Optional.of(Arrays.asList(FluentIterable.from(schemaProvider.getEdgeSchemas(type).get()).toArray(GraphEdgeSchema.class)));

            if (schemas.isPresent()) {
                for(GraphElementSchema schema : schemas.get()) {
                    searchBuilder.getIndices().addAll(FluentIterable.from(schema.getIndices()).toSet());
                }
            }
        }
    }

    public static <E extends Element> void applyTypes(
            SearchBuilder searchBuilder,
            GraphElementSchemaProvider schemaProvider,
            Class elementType) {

        Iterable<String> types = Vertex.class.isAssignableFrom(elementType) ?
                schemaProvider.getVertexTypes() : schemaProvider.getEdgeTypes();

        applyTypes(searchBuilder, schemaProvider, types, elementType);
    }

    public static <E extends Element> void applyTypes(
            SearchBuilder searchBuilder,
            GraphElementSchemaProvider schemaProvider,
            Iterable<String> types,
            Class elementType) {

        for(String type : types) {
            Optional<Iterable<? extends GraphElementSchema>> schemas = Vertex.class.isAssignableFrom(elementType) ?
                    Optional.of(Arrays.asList(schemaProvider.getVertexSchema(type).get())) :
                    Optional.of(Arrays.asList(FluentIterable.from(schemaProvider.getEdgeSchemas(type).get()).toArray(GraphEdgeSchema.class)));

            if (schemas.isPresent()) {
                for(GraphElementSchema schema : schemas.get()) {
                    searchBuilder.getTypes().add(schema.getType());
                }
            }
        }

        searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must()
                .terms("_type", searchBuilder.getTypes());
    }
}
