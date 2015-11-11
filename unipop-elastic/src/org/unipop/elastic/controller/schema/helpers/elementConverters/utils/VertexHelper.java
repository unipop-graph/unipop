package org.unipop.elastic.controller.schema.helpers.elementConverters.utils;

import com.google.common.collect.FluentIterable;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.schema.SchemaVertex;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.elasticsearch.common.base.Strings;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roman on 3/27/2015.
 */
public class VertexHelper {

    static public Vertex createVertex(Element element, GraphElementSchemaProvider schemaProvider, ElasticMutations elasticMutations, LazyGetter lazyGetter) {
        if (element.id() == null || Strings.isNullOrEmpty(element.id().toString())) {
            return ElasticMissingVertex.getInstance();
        }

        final Map<String, Object> keyValues = new HashMap<>();
        element.properties().forEachRemaining(property -> {
            keyValues.put(property.key(), property.value());
        });

        return new SchemaVertex(
                element.id(),
                element.label(),
                (UniGraph) element.graph(),
                keyValues,
                null,
                lazyGetter,
                schemaProvider,
                elasticMutations);
    }

    static public Vertex createVertex(Object id, String label, Map<String, Object> properties, UniGraph graph, GraphElementSchemaProvider schemaProvider, ElasticMutations elasticMutations, LazyGetter lazyGetter) {
        //TODO: repair
        if (id == null || Strings.isNullOrEmpty(id.toString())) {
            return ElasticMissingVertex.getInstance();
        }

        SchemaVertex vertex = new SchemaVertex(id, label, graph, null, null, lazyGetter, schemaProvider, elasticMutations);
        if (properties != null) {
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                vertex.property(property.getKey(), property.getValue());
            }
        }

        return vertex;
    }
}
