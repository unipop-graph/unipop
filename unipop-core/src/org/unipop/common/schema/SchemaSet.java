package org.unipop.common.schema;


import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashSet;
import java.util.Set;

public class SchemaSet<S extends ElementSchema<? extends Element>> extends HashSet<S> {

    public SchemaSet(Set<S> filteredSchemas) {
        super(filteredSchemas);
    }

}
