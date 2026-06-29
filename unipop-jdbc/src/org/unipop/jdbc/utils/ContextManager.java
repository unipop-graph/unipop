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
        return fetch(query, 0);
    }

    private List<Map<String, Object>> fetch(ResultQuery query, int count) {
        if (count > 3) {
            throw new IllegalArgumentException("query can't be fetched");
        }
        try {
            return context.fetch(query).intoMaps();
        } catch (Exception e) {
            closeQuietly();
        }
        try {
            reloadContexts();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fetch(query, ++count);
    }

    public int execute(Query query) {
        try {
            return context.execute(query);
        } catch (Exception e) {
            closeQuietly();
        }
        try {
            reloadContexts();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return execute(query);
    }

    public int execute(String query) {
        try {
            return context.execute(query);
        } catch (Exception e) {
            closeQuietly();
        }
        try {
            reloadContexts();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return execute(query);
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
        try {
            return context.render(query);
        } catch (Exception e) {
            closeQuietly();
        }
        try {
            reloadContexts();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return render(query);
    }

    public void batch(List<Query> bulk) {
        try {
            context.batch(bulk).execute();
            return;
        } catch (Exception e) {
            closeQuietly();
        }
        try {
            reloadContexts();
        } catch (IOException e) {
            e.printStackTrace();
        }
        batch(bulk);
    }
}
