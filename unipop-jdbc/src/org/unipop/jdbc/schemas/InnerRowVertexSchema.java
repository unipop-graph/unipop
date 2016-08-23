package org.unipop.jdbc.schemas;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.Query;
import org.json.JSONObject;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/22/16.
 */
public class InnerRowVertexSchema extends RowVertexSchema{
    public InnerRowVertexSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);
    }

    public InnerRowVertexSchema(JSONObject configuration, String table, UniGraph graph) {
        super(configuration, table, graph);
    }

    @Override
    protected Map<String, Object> getFields(Vertex element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        List<Map<String, Object>> fieldMaps = this.getPropertySchemas().stream().map(schema ->
                schema.toFields(properties)).filter(prop -> prop != null).collect(Collectors.toList());
        return ConversionUtils.merge(fieldMaps, this::mergeFields, false);
    }

    @Override
    public Query getInsertStatement(Vertex element) {
        return null;
    }
}
