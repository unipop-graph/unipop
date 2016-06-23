package org.unipop.jdbc;

import com.google.common.collect.Sets;
import org.jooq.Condition;
import org.jooq.CreateTableAsStep;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.property.PropertySchema;
import org.unipop.common.property.PropertySchemaFactory;
import org.unipop.common.schema.referred.ReferredVertexSchema;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.controller.simple.RowController;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 21/6/2016
 */
public class JdbcSourceProvider implements SourceProvider {
    private final Supplier<PredicatesTranslator<Iterable<Condition>>> predicatesTranslatorSupplier;
    private RowController controller;

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
//
//        for (RowVertexSchema sc : vertexSchemas) {
//            DSLContext dsl = DSL.using(c, dialect);
//            CreateTableAsStep step = DSL.using(c, dialect).createTable(DSL.table(sc.getTable()));
//            sc.getPropertySchemas()
//                    .stream()
//                    .map(PropertySchema::getFields)
//                    .flatMap(Collection::stream)
//                    .forEach(cn -> step.column(cn, SQLDataType.VARCHAR.length(500)));
//
//            dsl.execute(step);
//
//
//
//        }

        this.controller = new RowController(graph, c, dialect, vertexSchemas, edgeSchemas, predicatesTranslatorSupplier.get());
        return Sets.newHashSet(controller);
    }

    @Override
    public void close() {
        this.controller.getDslContext().close();
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
        String user = configuration.optString("user");
        String password = configuration.optString("password");

        if (user.isEmpty() && password.isEmpty()) {
            return DriverManager.getConnection(url);
        }
        return DriverManager.getConnection(url, user, password);
    }

    private List<JSONObject> getConfigs(JSONObject configuration, String key) throws JSONException {
        List<JSONObject> configs = new ArrayList<>();
        JSONArray configsArray = configuration.optJSONArray(key);


        for (int i = 0; i < configsArray.length(); i++) {
            JSONObject config = configsArray.getJSONObject(i);
            configs.add(config);
        }
        return configs;
    }
}
