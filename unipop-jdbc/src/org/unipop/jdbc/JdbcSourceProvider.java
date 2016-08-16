package org.unipop.jdbc;

import com.google.common.collect.Sets;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.json.JSONObject;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.controller.simple.RowController;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.query.controller.SourceProvider;
import org.unipop.query.controller.UniQueryController;
import org.unipop.schema.element.SchemaSet;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.unipop.util.ConversionUtils.getList;

/**
 * @author Gur Ronen
 * @since 21/6/2016
 */
public class JdbcSourceProvider implements SourceProvider {
    private final Supplier<PredicatesTranslator<Condition>> predicatesTranslatorSupplier;
    private UniGraph graph;
    private DSLContext context;

    public JdbcSourceProvider() {
        this(JdbcPredicatesTranslator::new);
    }

    public JdbcSourceProvider(Supplier<PredicatesTranslator<Condition>> predicatesTranslatorSupplier) {
        this.predicatesTranslatorSupplier = predicatesTranslatorSupplier;
    }

    @Override
    public Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception {
        Connection c = getConnection(configuration);
        SQLDialect dialect = SQLDialect.valueOf(configuration.getString("sqlDialect"));

        Configuration conf = new DefaultConfiguration().set(c).set(dialect)
                .set(new DefaultExecuteListenerProvider(new TimingExecuterListener()));

        this.context = DSL.using(conf);

        this.graph = graph;

        Set<JdbcSchema> schemas = Sets.newHashSet();

        getList(configuration, "vertices").forEach(vertexJson -> schemas.add(createVertexSchema(vertexJson)));
        getList(configuration, "edges").forEach(edgeJson -> schemas.add(createEdgeSchema(edgeJson)));

        return createControllers(schemas);
    }

    @Override
    public void close() {
        this.context.close();
    }

    public RowVertexSchema createVertexSchema(JSONObject vertexJson) {
        return new RowVertexSchema(vertexJson, this.graph);
    }

    public RowEdgeSchema createEdgeSchema(JSONObject edgeJson) {
        return new RowEdgeSchema(edgeJson, this.graph);
    }

    public Set<UniQueryController> createControllers(Set<JdbcSchema> schemas) {
        RowController rowController = new RowController(this.graph, this.context, schemas, this.predicatesTranslatorSupplier.get());
        return Sets.newHashSet(rowController);
    }

    private Connection getConnection(JSONObject configuration) throws SQLException, ClassNotFoundException {
        String url = configuration.getString("address");
        String driver = configuration.getString("driver");
        String user = configuration.optString("user");
        String password = configuration.optString("password");

        Class.forName(driver);
        if (user.isEmpty() && password.isEmpty()) {
            return DriverManager.getConnection(url);
        }
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public String toString() {
        return "JdbcSourceProvider{" +
                "context=" + context +
                ", predicatesTranslatorSupplier=" + predicatesTranslatorSupplier +
                ", graph=" + graph +
                '}';
    }
}
