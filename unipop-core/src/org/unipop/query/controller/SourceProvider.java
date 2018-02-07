package org.unipop.query.controller;

import org.json.JSONObject;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.traversalfilter.TraversalFilter;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A source provider turns mappings into controllers
 */
public interface SourceProvider {
    /**
     * Returns a set of controllers from a json mapping
     * @param graph The graph
     * @param configuration The mapping
     * @return A set of controllers
     * @throws Exception
     */
    Set<UniQueryController> init(UniGraph graph, JSONObject configuration, TraversalFilter filter) throws Exception;

    /**
     * Returns a list of additional provider property schema builders
     * @return List of property schema builders
     */
    default List<PropertySchema.PropertySchemaBuilder> providerBuilders() {return Collections.emptyList();}

    /**
     * Closes all resources
     */
    void close();
}
