package org.unipop.elastic2.controller.schema.helpers.schemaProviders;

import java.util.Optional;

/**
 * Created by Roman on 1/16/2015.
 */
public interface GraphElementSchema {
    public Class getSchemaElementType();

    public String getType();

    public Optional<GraphElementRouting> getRouting();

    public Iterable<String> getIndices();


}
