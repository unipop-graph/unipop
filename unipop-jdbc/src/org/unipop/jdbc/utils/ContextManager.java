package org.unipop.jdbc.utils;

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.jdbc.controller.simple.RowController;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 27/12/16.
 */
public class ContextManager {
    private final static Logger logger = LoggerFactory.getLogger(ContextManager.class);
    private Set<DSLContext> contexts;
    private JSONObject conf;

    public ContextManager(JSONObject conf) throws SQLException, IOException, ClassNotFoundException {
        this.conf = conf;
        reloadContexts();
    }

    private void reloadContexts() {
        this.contexts = new HashSet<>();
        SQLDialect dialect = SQLDialect.valueOf(this.conf.getString("sqlDialect"));
        List<Connection> connections = getConnections(this.conf);
        contexts = connections.stream().map(connection -> {
            Configuration conf = new DefaultConfiguration().set(connection).set(dialect)
                    .set(new DefaultExecuteListenerProvider(new TimingExecuterListener()));
            return DSL.using(conf);
        }).collect(Collectors.toSet());
    }

    private List<Connection> getConnections(JSONObject configuration) {
        List<Connection> connections = new ArrayList<>();
        try {
            List address = new ObjectMapper()
                    .readValue(configuration.getJSONArray("address").toString(), List.class);

            String driver = configuration.getString("driver");
            String user = configuration.optString("user");
            String password = configuration.optString("password");

            for (Object url : address) {
                try {
                    Class.forName(driver);
                    if (user.isEmpty() && password.isEmpty()) {
                        connections.add(DriverManager.getConnection(url.toString()));
                    } else
                        connections.add(DriverManager.getConnection(url.toString(), user, password));
                } catch (SQLException | ClassNotFoundException exception) {
                    logger.error(exception.getMessage());
                }
            }
        } catch (IOException exception){
            logger.error(exception.getMessage());
        }
        return connections;
    }

    public List<Map<String, Object>> fetch(ResultQuery query){
        for (DSLContext context : contexts) {
            try {
                return context.fetch(query).intoMaps();
            } catch (Exception e) {
                context.close();
                contexts.remove(context);
            }
        }
        reloadContexts();
        if (contexts.isEmpty()) {
            // TODO: write to log that no results were returned because no connections were established
            return Collections.emptyList();
        }
        return fetch(query);
    }

    public int execute(Query query){
        for (DSLContext context : contexts) {
            try {
                return context.execute(query);
            } catch (Exception e) {
                context.close();
                contexts.remove(context);
            }
        }
        reloadContexts();
        if (contexts.isEmpty()) {
            logger.error("No results were returned because no connections could be established for query=%s json=%s", query, conf);
            return 0;
        }
        return execute(query);
    }

    public int execute(String query){
        for (DSLContext context : contexts) {
            try {
                return context.execute(query);
            } catch (Exception e) {
                context.close();
                contexts.remove(context);
            }
        }
        reloadContexts();
        if (contexts.isEmpty()) {
            logger.error("No rows were changed because no connections could be established for query=%s json=%s", query, conf);
            return 0;
        }
        return execute(query);
    }

    public void close(){
        contexts.forEach(DSLContext::close);
    }

    public Object render(Query query) {
        for (DSLContext context : contexts) {
            try {
                return context.render(query);
            } catch (Exception e) {
                context.close();
                contexts.remove(context);
            }
        }
        reloadContexts();
        if (contexts.isEmpty()) {
            logger.error("could not render because no connections could be established for query=%s json=%s", query, conf);
            return "";
        }
        return render(query);
    }

    public void batch(List<Query> bulk) {
        for (DSLContext context : contexts) {
            try{
                context.batch(bulk).execute();
                return;
            }
            catch (Exception e) {
                context.close();
                contexts.remove(context);
            }
        }
        reloadContexts();
        if (contexts.isEmpty()) {
            logger.error("could not execute batch");
            return;
        }
        batch(bulk);
    }
}
