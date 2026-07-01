package test;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.test.UnipopGraphProvider;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Gur Ronen
 * @since 6/20/2016
 */
public class JdbcGraphProvider extends UnipopGraphProvider {
    static {
        // This provider is test-only but lives in main source, while the embedded PostgreSQL
        // server lives in test source (it needs the test-scope zonky dependency). Start it
        // reflectively here so there is no main->test/zonky compile dependency. A static
        // initializer runs on first instantiation, before this constructor body and before any
        // caller (suite runner / JdbcWorld) connects — so Postgres is always up in time.
        try {
            Class.forName("org.unipop.jdbc.suite.EmbeddedPostgresServer")
                    .getMethod("ensureStarted").invoke(null);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Could not start embedded PostgreSQL for tests", e);
        }
    }

    private final Connection jdbcConnection;

    public JdbcGraphProvider() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        this.jdbcConnection = DriverManager.getConnection(
                "jdbc:postgresql://localhost:54329/postgres", "postgres", "");

        createTables();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        String configurationFile = getSchemaConfiguration(loadGraphWith);
        URL url = this.getClass().getResource(configurationFile);
        String file = url.getFile();
        if (System.getProperty("os.name").toLowerCase().contains("windows"))
            file = file.substring(1);
        baseConfiguration.put("providers", file);
        return baseConfiguration;
    }

    private String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
        String confDirectory = "/configuration/" + System.getenv("conf") + "/";
        if (loadGraphWith != null)
            switch (loadGraphWith) {
                case MODERN:
                    return confDirectory + "modern";
                case GRATEFUL:
                    return confDirectory + "grateful";
                default:
                    return "/configuration/basic/default";
            }
        return "/configuration/basic/default";
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        super.clear(graph, configuration);
        truncateTables();
    }

    private void createTables() throws SQLException {
        //region dull tables
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS VERTEX_INNER(" +
                        "ID VARCHAR (100) NOT NULL," +
                        "LABEL VARCHAR(100) NOT NULL," +
                        "NAME VARCHAR(100)," +
                        "marko VARCHAR(100)," +
                        "josh VARCHAR(100)," +
                        "vadas VARCHAR(100)," +
                        "ripple VARCHAR(100)," +
                        "lop VARCHAR(100)," +
                        "peter VARCHAR(100)," +
                        "AGE INT," +
                        "LANG VARCHAR (100)," +
                        "KNOWNBY VARCHAR (100)," +
                        "CREATEDBY VARCHAR (100)," +
                        "EDGEID VARCHAR (100)," +
                        "EDGELANG VARCHAR (100)," +
                        "EDGEWEIGHT DOUBLE PRECISION," +
                        "EDGENAME VARCHAR(100))"
        );

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS vertices(" +
                        "ID VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "LABEL VARCHAR(100) NOT NULL," +
                        "name varchar(100), " +
                        "age int, " +
                        "location VARCHAR(100)," +
                        "status VARCHAR(100)," +
                        "oid int," +
                        "test VARCHAR(100)," +
                        "communityIndex int," +
                        "data VARCHAR(100)," +
                        // Typed columns for the TinkerPop 3.7+ GType/asNumber data-type feature
                        // scenarios (Data - DOUBLE/FLOAT/INT/LONG etc.): addV("data").property("<t>", v)
                        // on the empty graph must round-trip the value at its exact Java type, so each
                        // maps to the matching SQL type (DOUBLE PRECISION->Double, REAL->Float,
                        // INTEGER->Integer, BIGINT->Long). Column names are non-keyword (dval/fval/
                        // ival/lval) since jOOQ renders identifiers unquoted; the property key -> column
                        // mapping lives in default.json.
                        "dval DOUBLE PRECISION," +
                        "fval REAL," +
                        "ival INTEGER," +
                        "lval BIGINT," +
                        // birthday: stored as text, read back and parsed by asDate()/dateDiff().
                        // k: generic key used by the has(k, within(..)) unicode scenario.
                        "birthday VARCHAR(100)," +
                        "k VARCHAR(100)," +
                        // PartitionStrategy scenarios tag elements with a "_partition" property and
                        // filter reads by it (has("_partition", within(...))). The predicate translator
                        // keys off the property name, so the column is named "_partition" to match.
                        "_partition VARCHAR(100)," +
                        "lang VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS edges(" +
                        "ID VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "LABEL VARCHAR(100) NOT NULL, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "year INT," +
                        "acl VARCHAR(100)," +
                        "time VARCHAR(100)," +
                        "location VARCHAR(100)," +
                        "data VARCHAR(100)," +
                        "_partition VARCHAR(100)," +
                        // mergeE onCreate/onMatch scenarios tag edges with a "created" property.
                        "created VARCHAR(100)," +
                        "weight DOUBLE PRECISION)");
        //endregion

        //region modern tables
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS PERSON_MODERN(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name varchar(100), " +
                        "age int)");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS ANIMAL_MODERN(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name varchar(100), " +
                        "age int)");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS SOFTWARE_MODERN(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name varchar(100), " +
                        "lang VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS MODERN_EDGES(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "label VARCHAR(100) NOT NULL, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "year int," +
                        "weight DOUBLE PRECISION)");
        //endregion

        //region crew tables
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS PERSON_CREW(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name VARCHAR(100)," +
                        "location VARCHAR(100)," +
                        "lang VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS SOFTWARE_CREW(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS DEVELOPS_CREW(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "since int)");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS USES_CREW(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "skill int)");
        //endregion

        //region grateful dead
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS ARTIST_GRATEFUL_DEAD(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name VARCHAR(100))"
        );

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS SONG_GRATEFUL_DEAD(" +
                        "id VARCHAR(100) NOT NULL, " +
                        "name VARCHAR(100)," +
                        "songType VARCHAR(100)," +
                        "performances int)"
        );

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS GRATEFUL_DEAD_EDGES(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "label VARCHAR(100) NOT NULL," +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "weight DOUBLE PRECISION)"
        );
        //endregion
    }

    public void truncateTables() throws SQLException {
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE vertices");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE edges");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE vertex_inner");

        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE PERSON_MODERN");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE ANIMAL_MODERN");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE SOFTWARE_MODERN");

        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE PERSON_CREW");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE SOFTWARE_CREW");

        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE ARTIST_GRATEFUL_DEAD");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE SONG_GRATEFUL_DEAD");

        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE MODERN_EDGES");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE DEVELOPS_CREW");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE USES_CREW");
        this.jdbcConnection.createStatement().execute("TRUNCATE TABLE GRATEFUL_DEAD_EDGES");
    }
}
