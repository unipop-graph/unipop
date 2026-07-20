package org.unipop.schema.catalog;

import org.unipop.schema.element.ElementSchema;

import java.util.Set;

/**
 * Controllers that own {@link ElementSchema}s contribute them so the internal schema catalog
 * can materialize type topology. Not a user-facing Graph API.
 */
public interface SchemaContributor {

    /**
     * Root (and search-relevant) element schemas this controller will query.
     */
    Set<? extends ElementSchema> contributedSchemas();

    /**
     * Stable source id for catalog {@code boundTo} edges (e.g. provider file path or class name).
     */
    default String sourceId() {
        return getClass().getName();
    }

    /**
     * Provider / controller class name recorded on the catalog source vertex.
     */
    default String providerClass() {
        return getClass().getName();
    }

    /**
     * Whether this edge schema supports adjacency JOIN pushdown (exact class checks live here).
     */
    default boolean supportsJoin(ElementSchema schema) {
        return false;
    }
}
