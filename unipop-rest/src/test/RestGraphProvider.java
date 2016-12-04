package test;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.elastic.common.ElasticClient;

import java.util.Collections;

/**
 * Created by sbarzilay on 4/12/16.
 */
public class RestGraphProvider extends ElasticGraphProvider {
    public RestGraphProvider() throws Exception {
    }

    @Override
    public void loadGraphData(Graph graph, LoadGraphWith loadGraphWith, Class testClass, String testName) {
        ElasticClient client = new ElasticClient(Collections.singletonList("http://localhost:9200"));
        client.validateIndex("edge");
        client.validateIndex("vertex");
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }
}
