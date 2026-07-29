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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sbarzilay on 27/12/16.
 */
public class ContextManager {
    private final static Logger logger = LoggerFactory.getLogger(ContextManager.class);

    public final static long DEFAULT_LOCK_TIMEOUT_MILLIS = 30_000L;
    public final static long DEFAULT_CONNECTION_WAIT_MILLIS = 30_000L;
    public final static int DEFAULT_BATCH_SIZE = 1000;

    private DSLContext context;
    private Configuration jooqConfiguration;
    private BasicDataSource ds;
    private JSONObject conf;

    /**
     * The connection pinned for an open transaction, and a context bound to it. Thread-local because
     * one Gremlin request is iterated and committed on a single thread, and because two concurrent
     * requests must not share a transaction — each pins its own connection out of the same pool.
     */
    private final ThreadLocal<Connection> txConnection = new ThreadLocal<>();
    private final ThreadLocal<DSLContext> txContext = new ThreadLocal<>();

    /**
     * Every pinned connection, not just this thread's. A ThreadLocal cannot be enumerated, so without this
     * close() could only end the calling thread's transaction — and closing the pool does not end a
     * transaction the server still has open, leaving its locks held until the backend is reaped.
     */
    private final Set<Connection> pinnedConnections = java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Milliseconds a statement may wait for a lock before the server cancels it; 0 waits forever. */
    private final long lockTimeoutMillis;
    /** Milliseconds a caller may wait for a pooled connection before failing. */
    private final long connectionWaitMillis;
    /** Pool size, or 0 to leave the pooling library's own default in place. */
    private final int maxConnections;

    public ContextManager(JSONObject conf) throws SQLException, IOException, ClassNotFoundException, NamingException {
        this(conf, DEFAULT_LOCK_TIMEOUT_MILLIS, DEFAULT_CONNECTION_WAIT_MILLIS, 0);
    }

    public ContextManager(JSONObject conf, long lockTimeoutMillis, long connectionWaitMillis, int maxConnections)
            throws SQLException, IOException, ClassNotFoundException, NamingException {
        this.conf = conf;
        this.lockTimeoutMillis = lockTimeoutMillis;
        this.connectionWaitMillis = connectionWaitMillis;
        this.maxConnections = maxConnections;
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
        // A transaction pins a connection for its whole life, so the pool size is now the ceiling on
        // concurrent transactions rather than on concurrent statements. Left alone, exhaustion blocks
        // forever (BasicDataSource defaults maxWaitMillis to -1), turning an undersized pool into a hang
        // instead of an error — so the wait is always bounded. The size is only overridden when asked for,
        // because every graph builds its own pool: raising it multiplies across graphs and runs the server
        // out of max_connections instead.
        if (maxConnections > 0) ds.setMaxTotal(maxConnections);
        ds.setMaxWaitMillis(connectionWaitMillis);
        // maxWaitMillis bounds waiting for a *connection*, not waiting for a *row lock*. Holding a
        // transaction open means another writer can now block on our uncommitted rows indefinitely —
        // notably an "insert .. on conflict do nothing" of a key we inserted but have not committed, which
        // must wait to learn whether we commit. Without a cap that is an unbounded hang rather than an
        // error, so give lock waits a deadline. Postgres-only syntax, hence the dialect check.
        if (dialect == SQLDialect.POSTGRES) {
            ds.setConnectionInitSqls(java.util.Collections.singletonList(
                    "SET lock_timeout = " + lockTimeoutMillis));
        }
        Settings settings = new Settings();
        // jOOQ 3.12+ replaced RenderNameStyle.AS_IS with RenderNameCase + RenderQuotedNames.
        settings.setRenderNameCase(RenderNameCase.AS_IS);
        settings.setRenderQuotedNames(RenderQuotedNames.NEVER);
        Configuration conf = new DefaultConfiguration().set(ds).set(dialect)
                .set(settings)
                .set(new DefaultExecuteListenerProvider(new TimingExecuterListener()));
        this.jooqConfiguration = conf;
        this.context = DSL.using(conf);
    }

    public boolean isTransactionOpen() {
        return txConnection.get() != null;
    }

    /** Idempotent: a transaction is already open on this thread means we are already inside it. */
    public void beginTransaction() {
        if (isTransactionOpen()) return;
        Connection connection = null;
        try {
            connection = ds.getConnection();
            connection.setAutoCommit(false);
            pinnedConnections.add(connection);
            txConnection.set(connection);
            // derive() keeps the dialect, render settings and timing listener, and swaps in this one connection
            txContext.set(DSL.using(jooqConfiguration.derive(connection)));
        } catch (final SQLException | RuntimeException e) {
            // Failing anywhere after getConnection() must not strand it: at that point it is checked out of
            // the pool but not yet reachable through either the thread-local or the pinned set, so nothing
            // downstream would ever hand it back.
            if (connection != null) {
                pinnedConnections.remove(connection);
                txConnection.remove();
                txContext.remove();
                try {
                    connection.close();
                } catch (final SQLException closeFailure) {
                    logger.warn("could not return a half-opened transaction's connection to the pool", closeFailure);
                }
            }
            throw new IllegalStateException("could not open a transaction", e);
        }
    }

    public void commitTransaction() {
        endTransaction(true);
    }

    public void rollbackTransaction() {
        endTransaction(false);
    }

    private void endTransaction(final boolean commit) {
        Connection connection = txConnection.get();
        if (connection == null) return;
        try {
            if (commit) connection.commit();
            else connection.rollback();
        } catch (final SQLException e) {
            throw new IllegalStateException("could not " + (commit ? "commit" : "roll back") + " the transaction", e);
        } finally {
            // released even when commit throws, or the thread keeps a broken connection for every later request
            txConnection.remove();
            txContext.remove();
            pinnedConnections.remove(connection);
            try {
                connection.setAutoCommit(true);
                connection.close();
            } catch (final SQLException e) {
                logger.warn("failed to return the transaction's connection to the pool", e);
            }
        }
    }

    private DSLContext context() {
        DSLContext pinned = txContext.get();
        return pinned != null ? pinned : context;
    }

    public List<Map<String, Object>> fetch(ResultQuery query) {
        return withRetry("fetch", () -> context().fetch(query).intoMaps());
    }

    /**
     * Run a jOOQ operation, transparently closing and rebuilding the context on failure.
     * Retries are BOUNDED: execute/render/batch were previously unbounded recursions that
     * spin forever when an operation persistently fails (e.g. an incompatible statement),
     * so the original cause is logged each attempt and rethrown after the cap rather than
     * hanging.
     */
    private <T> T withRetry(final String op, final java.util.function.Supplier<T> action) {
        // Never reconnect underneath an open transaction: rebuilding the DataSource would discard it and
        // re-run this one statement on a fresh auto-committing connection, tearing an atomic unit in half
        // and reporting success. A failure inside a transaction has to reach the caller so it can roll back.
        if (isTransactionOpen()) return action.get();

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
        return withRetry("execute", () -> context().execute(query));
    }

    public int execute(String query) {
        return withRetry("execute", () -> context().execute(query));
    }

    public void close() {
        // this thread's transaction first, so it goes through the normal release path
        if (isTransactionOpen()) rollbackTransaction();
        // then any left open on other threads: closing the pool returns the connection but does not end the
        // transaction the server still has open, so its locks would outlive this graph entirely
        for (final Connection pinned : pinnedConnections) {
            try {
                pinned.rollback();
                pinned.setAutoCommit(true);
                pinned.close();
            } catch (final SQLException e) {
                logger.warn("could not roll back a transaction left open on another thread", e);
            }
        }
        pinnedConnections.clear();
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
        return withRetry("render", () -> context().render(query));
    }

    /**
     * Render a query as inlined SQL under this context's dialect — the same form
     * TimingExecuterListener keys its map by ({@code ctx.dsl().renderInlined(ctx.query())}). A
     * detached query renders with the DEFAULT dialect (e.g. {@code limit}); this renders with the
     * configured dialect (e.g. Postgres {@code fetch next ... rows only}) so a caller can reconstruct
     * the exact timing-map key.
     */
    public String renderInlined(Query query) {
        return withRetry("renderInlined", () -> context().renderInlined(query));
    }

    public void batch(List<Query> bulk) {
        withRetry("batch", () -> {
            context().batch(bulk).execute();
            return null;
        });
    }
}
