package org.unipop.schema.catalog;

/**
 * Controllers that use the internal schema catalog for pruning / pushdown decisions.
 */
public interface SchemaCatalogAware {

    void setSchemaCatalog(SchemaCatalog catalog);
}
