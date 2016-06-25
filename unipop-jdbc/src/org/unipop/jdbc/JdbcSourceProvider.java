package org.unipop.jdbc;

import com.google.common.collect.Sets;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.common.util.SchemaSet;
import org.unipop.jdbc.controller.simple.RowController;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.builders.RowEdgeBuilder;
import org.unipop.jdbc.schemas.builders.RowVertexBuilder;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.unipop.common.util.ConversionUtils.getList;

/**
 * @author Gur Ronen
 * @since 21/6/2016
 */
public class JdbcSourceProvider implements SourceProvider {
    private final Supplier<PredicatesTranslator<Iterable<Condition>>> predicatesTranslatorSupplier;
    private UniGraph graph;
    private DSLContext context;

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

        this.context = DSL.using(c, dialect);
        this.graph = graph;

        SchemaSet schemas = new SchemaSet();
        getList(configuration, "vertices").forEach(vertexJson -> schemas.add(createVertexSchema(vertexJson)));
        getList(configuration, "edges").forEach(edgeJson -> schemas.add(createEdgeSchema(edgeJson)));

        return createControllers(schemas);
    }

    @Override
    public void close() {
        this.context.close();
    }

    public RowVertexSchema createVertexSchema(JSONObject vertexJson) {
        return new RowVertexBuilder(vertexJson, this.graph).build();
    }

    public RowEdgeSchema createEdgeSchema(JSONObject edgeJson) {
        return new RowEdgeBuilder(edgeJson, this.graph).build();
    }

    public Set<UniQueryController> createControllers(SchemaSet schemas) {
        RowController rowController = new RowController(this.graph, this.context, schemas, this.predicatesTranslatorSupplier.get());
        return Sets.newHashSet(rowController);
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

}
