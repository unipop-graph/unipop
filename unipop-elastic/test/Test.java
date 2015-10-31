import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controllermanagers.ElasticStarControllerManager;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.structure.UniGraph;

/**
 * Created by sbarzilay on 10/31/15.
 */
public class Test {
    UniGraph graph;

    public Test() throws InstantiationException {
        BaseConfiguration conf = new BaseConfiguration();
        conf.addProperty("controllerManager", ElasticStarControllerManager.class.getCanonicalName());
        conf.addProperty("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
        graph = new UniGraph(conf);
    }

    @org.junit.Test
    public void test() throws Exception {
        Vertex marko = graph.addVertex("person");
        marko.property("name", "marko");
        marko.property("age", "29");

        Vertex josh = graph.addVertex("person");
        marko.property("name", "josh");
        marko.property("age", "32");

        Vertex vadas = graph.addVertex("person");
        marko.property("name", "vadas");
        marko.property("age", "27");

        Vertex peter = graph.addVertex("person");
        marko.property("name", "peter");
        marko.property("age", "35");

        Vertex lop = graph.addVertex("software");
        marko.property("name", "lop");
        marko.property("lang", "java");

        Vertex ripple = graph.addVertex("software");
        marko.property("name", "ripple");
        marko.property("lang", "java");

        marko.addEdge("created", lop, "weight", 0.4d);
        marko.addEdge("knows", vadas, "weight", 0.5d);
        marko.addEdge("knows", josh, "weight", 1.0d);

        josh.addEdge("created", lop, "weight", 0.4d);
        josh.addEdge("created", ripple, "weight", 1.0d);

        peter.addEdge("created", lop, "weight", 0.2d);
        graph.traversal().V().both().both().count()
                .forEachRemaining(System.out::println);
    }
}
