package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.integration.controllermanagers.IntegrationControllerManager;
import org.unipop.process.strategyregistrar.StandardStrategyRegistrar;
import org.unipop.structure.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class IntegrationGraphProvider extends AbstractGraphProvider {

    private static String CLUSTER_NAME = "test";

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(UniEdge.class);
        add(UniElement.class);
        add(UniGraph.class);
        add(UniProperty.class);
        add(UniVertex.class);
        add(UniVertexProperty.class);
    }};

    private final Client client;
    private final Connection jdbcConnection;

    public IntegrationGraphProvider() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        //patch for failing IO tests that wrute to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
        //Delete elasticsearch 'data' directory
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        Node node = ElasticClientFactory.createNode(CLUSTER_NAME, false, 0);
        client = node.client();

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
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.cluster.address", "127.0.0.1:" + client.settings().get("transport.tcp.port"));
            put("controllerManagerFactory", (ControllerManagerFactory) IntegrationControllerManager::new);
            put("strategyRegistrar", new StandardStrategyRegistrar());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
//            String indexName = configuration.getString("graphName");
//            ElasticHelper.clearIndex(client, indexName);
            g.close();
            jdbcConnection.createStatement().execute("DELETE FROM PERSON;");
            jdbcConnection.createStatement().execute("DELETE FROM SOFTWARE;");
            jdbcConnection.createStatement().execute("DELETE FROM KNOWS;");
            jdbcConnection.createStatement().execute("DELETE FROM CREATED;");
            jdbcConnection.createStatement().execute("DELETE FROM ARTIST;");
            jdbcConnection.createStatement().execute("DELETE FROM SONG;");
            jdbcConnection.createStatement().execute("DELETE FROM WRITTERBY;");
            jdbcConnection.createStatement().execute("DELETE FROM SUNGBY;");
            jdbcConnection.createStatement().execute("DELETE FROM FOLLOWEDBY;");
            jdbcConnection.createStatement().execute("VACUUM");
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }

}
