package org.unipop.jdbc;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.test.UnipopGraphProvider;

import java.io.File;
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
    private static final String CONFIGURATION = "basic.json";

    private final Connection jdbcConnection;

    public JdbcGraphProvider() throws SQLException, ClassNotFoundException {
        new File("test.sqlite").delete();

        Class.forName("org.sqlite.JDBC");
        this.jdbcConnection = DriverManager.getConnection("jdbc:sqlite:test.sqlite");

        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS PERSON(id int , name varchar(100), age int, knows int, edgeid int, weight DOUBLE , created int);");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS animal(id int NOT NULL PRIMARY KEY, name varchar(100), age int);");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS SOFTWARE(id int , name varchar(100), lang VARCHAR(100), created int, edgeid int, weight DOUBLE);");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS ARTIST(id int NOT NULL PRIMARY KEY, name varchar(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS SONG(id int NOT NULL PRIMARY KEY, name varchar(100), songType VARCHAR(100), performances int);");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS KNOWS(id VARCHAR(100) NOT NULL PRIMARY KEY, weight DOUBLE , inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS CREATED(id VARCHAR(100) NOT NULL PRIMARY KEY, weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS FOLLOWEDBY(id VARCHAR(100) NOT NULL PRIMARY KEY, weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS SUNGBY(id VARCHAR(100) NOT NULL PRIMARY KEY, weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS WRITTERBY(id VARCHAR(100) NOT NULL PRIMARY KEY, weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS createdBy(id VARCHAR(100) NOT NULL PRIMARY KEY, acl VARCHAR(100) ,weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS codeveloper(id VARCHAR(100) NOT NULL PRIMARY KEY, year int, weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS existsWith(id VARCHAR(100) NOT NULL PRIMARY KEY, time VARCHAR(100), weight DOUBLE, inid int, inlabel VARCHAR(100), outid int, outlabel VARCHAR(100));");
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> baseConfiguration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        URL url = this.getClass().getResource("/configuration/" + CONFIGURATION);
        baseConfiguration.put("providers", new String[]{url.getFile()});

        new File("test.sqlite").delete();

        try {
            this.jdbcConnection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS vertices(" +
                            "ID VARCHAR(100) NOT NULL PRIMARY KEY, " +
                            "LABEL VARCHAR(100) NOT NULL," +
                            "name varchar(100), " +
                            "age int, " +
                            "location VARCHAR(100)," +
                            "status VARCHAR(100)," +
                            "lang VARCHAR(100))");

            this.jdbcConnection.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS edges(" +
                            "ID VARCHAR(100) NOT NULL PRIMARY KEY, " +
                            "LABEL NOT NULL, " +
                            "outId VARCHAR(100), " +
                            "outLabel VARCHAR(100), " +
                            "inId VARCHAR(100), " +
                            "inLabel VARCHAR(100)," +
                            "year VARCHAR(100)," +
                            "weight DOUBLE)");
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return baseConfiguration;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        this.jdbcConnection.createStatement().execute("DELETE FROM vertices");
        this.jdbcConnection.createStatement().execute("DELETE FROM edges");
    }
}
