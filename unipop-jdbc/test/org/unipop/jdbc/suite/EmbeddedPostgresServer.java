package org.unipop.jdbc.suite;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * Starts a single embedded PostgreSQL instance per JVM (on a fixed port) for the JDBC test
 * suites. Triggered from test sources only, so {@code test.JdbcGraphProvider} (main source)
 * stays {@code java.sql}-only and {@code embedded-postgres} remains a test-scope dependency.
 */
public final class EmbeddedPostgresServer {

    public static final int PORT = 54329;
    public static final String USER = "postgres";
    public static final String URL = "jdbc:postgresql://localhost:" + PORT + "/postgres";

    private static volatile EmbeddedPostgres instance;

    private EmbeddedPostgresServer() {
    }

    /** Idempotently start the embedded PostgreSQL server (once per JVM). */
    public static synchronized void ensureStarted() {
        if (instance != null) {
            return;
        }
        try {
            instance = EmbeddedPostgres.builder().setPort(PORT).start();
            Runtime.getRuntime().addShutdownHook(new Thread(EmbeddedPostgresServer::stop));
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to start embedded PostgreSQL on port " + PORT, e);
        }
    }

    private static synchronized void stop() {
        if (instance != null) {
            try {
                instance.close();
            } catch (final Exception ignored) {
            }
            instance = null;
        }
    }
}
