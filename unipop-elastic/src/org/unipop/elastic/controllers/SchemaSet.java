package org.unipop.elastic.controllers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.elastic.schema.ElementSchema;

import java.util.Set;

/**
 * Created by ranma on 23/03/2016.
 */
public class SchemaSet<E extends Element> {
    private Set<ElementSchema<E>> elementSchemas;

    public SchemaSet(Set<ElementSchema<E>> elementSchemas) {
        this.elementSchemas = elementSchemas;
    }

    public Set<ElementSchema<E>> getElementSchemas() {
        return elementSchemas;
    }
}
