package org.unipop.jdbc;

import com.google.common.collect.Sets;
import org.apache.commons.lang.NotImplementedException;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.property.PropertySchemaFactory;
import org.unipop.common.schema.referred.ReferredVertexSchema;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.controller.schemas.RowEdgeSchema;
import org.unipop.jdbc.controller.schemas.RowVertexSchema;
import org.unipop.jdbc.simple.RowController;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 21/6/2016
 */
public class JdbcSourceProvider  implements SourceProvider {
    private final Supplier<PredicatesTranslator<Iterable<Condition>>> predicatesTranslatorSupplier;

    public JdbcSourceProvider() {
        this(JdbcPredicatesTranslator::new);
    }

    public JdbcSourceProvider(Supplier<PredicatesTranslator<Iterable<Condition>>> predicatesTranslatorSupplier) {
        this.predicatesTranslatorSupplier = predicatesTranslatorSupplier;
    }

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        Connection c = getConnection(configuration);
        SQLDialect dialect = SQLDialect.valueOf(configuration.getString("sqlDialect"));

        Set<RowVertexSchema> vertexSchemas = extractVertexSchemas(graph, configuration);
        Set<RowEdgeSchema> edgeSchemas = extractEdgeSchemas(graph, configuration);

        RowController controller = new RowController(graph, c, dialect, vertexSchemas, edgeSchemas, new JdbcPredicatesTranslator());
        return Sets.newHashSet(controller);
    }

    private Set<RowEdgeSchema> extractEdgeSchemas(UniGraph graph, JSONObject configuration) {
        List<JSONObject> edges = getConfigs(configuration, "edges");
        return edges.stream().map(edge -> {
            ReferredVertexSchema outVertexSchema = extractEdgeInnerVertexSchema(graph, edge, "outVertex");
            ReferredVertexSchema inVertexSchema = extractEdgeInnerVertexSchema(graph, edge, "inVertex");

            String table = edge.getString("table");
            List<PropertySchema> propertySchemas = PropertySchemaFactory.createPropertySchemas(edge);

            return new RowEdgeSchema(outVertexSchema, inVertexSchema, propertySchemas, graph, table);
        }).collect(Collectors.toSet());
    }

    private ReferredVertexSchema extractEdgeInnerVertexSchema(UniGraph graph, JSONObject edge, String key) {
        JSONObject vertexSchema = edge.getJSONObject(key);
        List<PropertySchema> propertySchemas = PropertySchemaFactory.createPropertySchemas(vertexSchema);
        return new ReferredVertexSchema(propertySchemas, graph);
    }

    @Override
    public void close() {
        throw new NotImplementedException();
    }

    private Set<RowVertexSchema> extractVertexSchemas(UniGraph graph, JSONObject configuration) {
        List<JSONObject> vertices = getConfigs(configuration, "vertices");
        return vertices.stream().map(vertex -> {
            String table = vertex.getString("table");
            List<PropertySchema> propertySchemas = PropertySchemaFactory.createPropertySchemas(vertex);

            return new RowVertexSchema(propertySchemas, graph, table);
        }).collect(Collectors.toSet());
    }

    private Connection getConnection(JSONObject configuration) throws SQLException {
        String url = configuration.getString("address");
        String user = configuration.getString("user");
        String password = configuration.getString("password");

        if (user == null && password == null) {
            return DriverManager.getConnection(url);
        }
        return DriverManager.getConnection(url, user, password);
    }

    private List<JSONObject> getConfigs(JSONObject configuration, String key) throws JSONException {
        List<JSONObject> configs = new ArrayList<>();
        JSONArray configsArray = configuration.optJSONArray(key);
        for(int i = 0; i < configsArray.length(); i++){
            JSONObject config = configsArray.getJSONObject(i);
            configs.add(config);
        }
        return configs;
    }
}
