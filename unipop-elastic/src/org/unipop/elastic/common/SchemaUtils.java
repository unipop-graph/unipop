package org.unipop.elastic.common;

import org.apache.commons.lang3.tuple.Pair;
import org.unipop.elastic.document.DocumentEdgeSchema;
import org.unipop.elastic.document.DocumentSchema;
import org.unipop.elastic.document.DocumentVertexSchema;
import org.unipop.schema.element.ElementSchema;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 7/7/2016
 */
public final class SchemaUtils {
    public static Pair<Set<DocumentVertexSchema>, Set<DocumentEdgeSchema>> extractDocumentSchemas(Set<DocumentSchema> schemas) {
        Set<DocumentSchema> documentSchemas = collectSchemas(schemas);
        Set<DocumentVertexSchema> vertexSchemas = documentSchemas.stream().filter(schema -> schema instanceof DocumentVertexSchema)
                .map(schema -> ((DocumentVertexSchema)schema)).collect(Collectors.toSet());
        Set<DocumentEdgeSchema> edgeSchemas = documentSchemas.stream().filter(schema -> schema instanceof DocumentEdgeSchema)
                .map(schema -> ((DocumentEdgeSchema)schema)).collect(Collectors.toSet());

        return Pair.of(vertexSchemas, edgeSchemas);

    }

    private static Set<DocumentSchema> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<DocumentSchema> docSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if(schema instanceof DocumentSchema) {
                docSchemas.add((DocumentSchema) schema);
                Set<DocumentSchema> childSchemas = collectSchemas(schema.getChildSchemas());
                docSchemas.addAll(childSchemas);
            }
        });
        return docSchemas;
    }
}
