package org.unipop.jdbc.schemas;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.List;
import java.util.Map;

/**
 * @author Gur Ronen
 * @since 6/12/2016.
 */
public class RowVertexSchema extends AbstractRowSchema<Vertex> implements JdbcVertexSchema {
    public RowVertexSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);
    }

    @Override
    public List<Vertex> parseResults(String result, PredicateQuery query) {
        return null;
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if(properties == null) return null;
        return new UniVertex(properties, graph);
    }


    @Override
    public String toString() {
        return "RowVertexSchema{} " + super.toString();
    }
}
