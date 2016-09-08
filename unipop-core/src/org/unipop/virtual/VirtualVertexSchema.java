package org.unipop.virtual;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.NonDynamicPropertySchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.PropertySchemaFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by sbarzilay on 9/6/16.
 */
public class VirtualVertexSchema extends AbstractElementSchema<Vertex> implements VertexSchema{

    public VirtualVertexSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);
//        propertySchemas.forEach(schema -> {
//            if (schema instanceof NonDynamicPropertySchema)
//                schema.excludeDynamicProperties().add(T.id.getAccessor());
//        });
//        addPropertySchema(T.id.getAccessor(), "@" + T.id.getAccessor());
    }

    @Override
    public String getFieldByPropertyKey(String key) {
        return null;
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        return new UniVertex(properties, graph);
    }
}
