package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.unipop.test.UnipopGraphProvider;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Gur Ronen
 * @since 7/10/16
 */
public class IntegGraphProvider extends UnipopGraphProvider {
    private Connection jdbcConnection;
    private ElasticLocalNode localNode;

    private final File dataPath;

    public IntegGraphProvider() throws Exception {
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");

        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        this.dataPath = new File(path);

        this.localNode = new ElasticLocalNode(dataPath);

        Class.forName("org.h2.Driver");
        this.jdbcConnection = DriverManager.getConnection("jdbc:h2:gremlin;WRITE_DELAY=0;LOCK_MODE=3;");

        createTables();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        String configurationFile = getSchemaConfiguration(loadGraphWith);

        URL jdbcUrl = this.getClass().getResource("/configuration/jdbc/" + configurationFile);
        URL elasticUrl = this.getClass().getResource("/configuration/elastic/" + configurationFile);

        baseConfiguration.put("providers", new String[]{jdbcUrl.getFile(), elasticUrl.getFile()});

        return baseConfiguration;
    }

    @Override
    public void clear(Graph g, Configuration configuration) throws Exception {
        super.clear(g, configuration);
        clearJdbc();
        clearElastic();
    }

    private void createTables() throws SQLException {
        //region modern tables
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS PERSON_MODERN(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "name varchar(100), " +
                        "age int)");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS SOFTWARE_MODERN(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "name varchar(100), " +
                        "lang VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS MODERN_EDGES(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "label VARCHAR(100) NOT NULL, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "weight DOUBLE)");
        //endregion

        //region crew tables
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS PERSON_CREW(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "name VARCHAR(100)," +
                        "location VARCHAR(100)," +
                        "lang VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS SOFTWARE_CREW(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "name VARCHAR(100))");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS DEVELOPS_CREW(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "since int)");

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS USES_CREW(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "outId VARCHAR(100), " +
                        "outLabel VARCHAR(100), " +
                        "inId VARCHAR(100), " +
                        "inLabel VARCHAR(100)," +
                        "skill int)");
        //endregion

        //region grateful dead
        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS ARTIST_GRATEFUL_DEAD(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
                        "name VARCHAR(100))"
        );

        this.jdbcConnection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS SONG_GRATEFUL_DEAD(" +
                        "id VARCHAR(100) NOT NULL PRIMARY KEY, " +
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
                        "weight DOUBLE)"
        );

        //endregion
    }

    public String getSchemaConfiguration(LoadGraphWith.GraphData loadGraphWith) {
        if (loadGraphWith != null) {
            switch (loadGraphWith) {
                case MODERN: return "modern.json";
//                case CREW: return CrewConfiguration;
//                case GRATEFUL: return GratefulConfiguration;
            }
        }
        return "modern.json";
    }

    private void clearElastic() {
        if(localNode != null) {
            localNode.deleteIndices();
        }
    }

    private void clearJdbc() throws SQLException {
        this.jdbcConnection.createStatement().execute("DROP TABLE PERSON_MODERN");
        this.jdbcConnection.createStatement().execute("DROP TABLE SOFTWARE_MODERN");

        this.jdbcConnection.createStatement().execute("DROP TABLE PERSON_CREW");
        this.jdbcConnection.createStatement().execute("DROP TABLE SOFTWARE_CREW");

        this.jdbcConnection.createStatement().execute("DROP TABLE ARTIST_GRATEFUL_DEAD");
        this.jdbcConnection.createStatement().execute("DROP TABLE SONG_GRATEFUL_DEAD");

        this.jdbcConnection.createStatement().execute("DROP TABLE MODERN_EDGES");
        this.jdbcConnection.createStatement().execute("DROP TABLE DEVELOPS_CREW");
        this.jdbcConnection.createStatement().execute("DROP TABLE USES_CREW");
        this.jdbcConnection.createStatement().execute("DROP TABLE GRATEFUL_DEAD_EDGES");

        createTables();
    }

    public Client getClient() {
        return localNode.getClient();
    }
}
