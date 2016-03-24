package org.unipop.elastic.controllerprovider;

import org.apache.commons.configuration.BaseConfiguration;
import org.unipop.controller.Predicates;
import org.unipop.schema.Schema;

import java.util.Map;

public abstract class BasicSchema implements Schema {
    private String[] indices;

    public BasicSchema(BaseConfiguration configuration) {
        indices = configuration.<String>getStringArray("index");
    }

    public String[] getIndices(Predicates predicates) {
        return indices;
    }
}
