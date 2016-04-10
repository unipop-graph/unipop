package org.unipop.common.schema.base;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.common.schema.VertexSchema;
import org.unipop.common.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.List;
import java.util.Map;

public class BaseVertexSchema extends BaseElementSchema<Vertex> implements VertexSchema {
    public BaseVertexSchema(Map<String, PropertySchema> properties, boolean dynamicProperties, UniGraph graph) {
        super(properties, dynamicProperties, graph);
    }

    @Override
    public Vertex fromFields(Map fields) {
        Map properties = getProperties(fields);
        return new UniVertex(properties, graph);
    }

    @Override
    public PredicatesHolder toPredicates(List<Vertex> vertices) {

    }
}
