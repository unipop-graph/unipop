package org.unipop.elastic2.controller.schema.helpers.schemaProviders;

import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Optional;

/**
 * Created by Roman on 1/16/2015.
 */
public interface GraphEdgeSchema extends GraphElementSchema {
    default Class getSchemaElementType() {
        return Edge.class;
    }

    public interface End {
        public String getIdField();
        public Optional<String> getType();
    }

    public interface Direction {
        public String getField();
        public Object getInValue();
        public Object getOutValue();
    }

    public Optional<End> getSource();
    public Optional<End> getDestination();

    public Optional<Direction> getDirection();

}
