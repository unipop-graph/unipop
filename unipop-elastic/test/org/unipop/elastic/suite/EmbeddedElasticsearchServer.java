package org.unipop.elastic.suite;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ExpandWildcard;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

/** One Testcontainers ES 8 node per JVM (mirrors jdbc's EmbeddedPostgresServer). */
public final class EmbeddedElasticsearchServer {

    public static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.15.3";

    private static volatile ElasticsearchContainer container;
    private static volatile ElasticsearchClient client;
    private static volatile RestClient restClient;

    private EmbeddedElasticsearchServer() {}

    public static synchronized void ensureStarted() {
        if (container != null) return;
        ElasticsearchContainer c = new ElasticsearchContainer(DockerImageName.parse(IMAGE))
                .withEnv("xpack.security.enabled", "false")
                .withEnv("action.destructive_requires_name", "false")
                .withEnv("discovery.type", "single-node");
        c.start();
        String host = c.getHost();
        int port = c.getMappedPort(9200);
        RestClient rc = RestClient.builder(new HttpHost(host, port, "http")).build();
        ElasticsearchClient ec = new ElasticsearchClient(new RestClientTransport(rc, new JacksonJsonpMapper()));
        container = c;
        restClient = rc;
        client = ec;
        Runtime.getRuntime().addShutdownHook(new Thread(EmbeddedElasticsearchServer::stop));
    }

    public static String getHttpAddress() {
        ensureStarted();
        return "http://" + container.getHost() + ":" + container.getMappedPort(9200);
    }

    public static ElasticsearchClient getClient() {
        ensureStarted();
        return client;
    }

    /** Delete all non-system indices (per-scenario reset). Requires action.destructive_requires_name=false. */
    public static void deleteAllIndices() {
        ensureStarted();
        try {
            client.indices().delete(d -> d
                    .index("*")
                    .ignoreUnavailable(true)
                    .allowNoIndices(true)
                    .expandWildcards(ExpandWildcard.Open, ExpandWildcard.Closed));
        } catch (IOException e) {
            throw new IllegalStateException("failed to delete indices", e);
        }
    }

    private static void stop() {
        try { if (restClient != null) restClient.close(); } catch (IOException ignored) {}
        if (container != null) container.stop();
    }
}
