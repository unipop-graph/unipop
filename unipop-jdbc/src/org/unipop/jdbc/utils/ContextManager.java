package org.unipop.jdbc.utils;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.jooq.*;
import org.jooq.conf.RenderNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by sbarzilay on 27/12/16.
 */
public class ContextManager {
    private final static Logger logger = LoggerFactory.getLogger(ContextManager.class);
    private DSLContext context;
    private BasicDataSource ds;
    private JSONObject conf;

    public ContextManager(JSONObject conf) throws SQLException, IOException, ClassNotFoundException, NamingException {
        this.conf = conf;
        reloadContexts();
    }

    private void reloadContexts() throws IOException {
        SQLDialect dialect = SQLDialect.valueOf(this.conf.getString("sqlDialect"));
        closeQuietly();
        ds = new BasicDataSource();
        ds.setUrl(new ObjectMapper()
                .readValue(conf.getJSONArray("address").toString(), List.class).get(0).toString());
        ds.setDriverClassName(conf.getString("driver"));
        String user = conf.optString("user");
        String password = conf.optString("password");
        if (!user.isEmpty()) ds.setUsername(user);
        if (!password.isEmpty()) ds.setPassword(password);
        Settings settings = new Settings();
        // jOOQ 3.12+ replaced RenderNameStyle.AS_IS with RenderNameCase + RenderQuotedNames.
        settings.setRenderNameCase(RenderNameCase.AS_IS);
        settings.setRenderQuotedNames(RenderQuotedNames.NEVER);
        Configuration conf = new DefaultConfiguration().set(ds).set(dialect)
                .set(settings)
                .set(new DefaultExecuteListenerProvider(new TimingExecuterListener()));
        this.context = DSL.using(conf);
    }

    public List<Map<String, Object>> fetch(ResultQuery query) {
        return withRetry("fetch", () -> context.fetch(query).intoMaps());
    }

    /**
     * Run a jOOQ operation, transparently closing and rebuilding the context on failure.
     * Retries are BOUNDED: execute/render/batch were previously unbounded recursions that
     * spin forever when an operation persistently fails (e.g. an incompatible statement),
     * so the original cause is logged each attempt and rethrown after the cap rather than
     * hanging.
     */
    private <T> T withRetry(final String op, final java.util.function.Supplier<T> action) {
        Exception last = null;
        for (int attempt = 1; attempt <= 4; attempt++) {
            try {
                return action.get();
            } catch (final Exception e) {
                last = e;
                logger.warn("jOOQ {} failed (attempt {}/4), reconnecting", op, attempt, e);
                closeQuietly();
                try {
                    reloadContexts();
                } catch (final IOException re) {
                    logger.error("Failed to reload jOOQ context", re);
                }
            }
        }
        throw new IllegalStateException("jOOQ " + op + " failed after 4 attempts", last);
    }

    public int execute(Query query) {
        return withRetry("execute", () -> context.execute(query));
    }

    public int execute(String query) {
        return withRetry("execute", () -> context.execute(query));
    }

    public void close() {
        closeQuietly();
    }

    /**
     * jOOQ 3.19's DSLContext is no longer AutoCloseable; the underlying pooled
     * BasicDataSource owns the connections, so close it on reload/shutdown.
     */
    private void closeQuietly() {
        if (ds != null) {
            try {
                ds.close();
            } catch (SQLException ignored) {
            }
            ds = null;
        }
    }

    public Object render(Query query) {
        return withRetry("render", () -> context.render(query));
    }

    public void batch(List<Query> bulk) {
        withRetry("batch", () -> {
            context.batch(bulk).execute();
            return null;
        });
    }
}
