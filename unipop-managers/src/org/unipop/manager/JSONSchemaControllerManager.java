package org.unipop.manager;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.SchemaControllerManager;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

/**
 * Created by sbarzilay on 30/01/16.
 */
public class JSONSchemaControllerManager extends SchemaControllerManager {

    @SuppressWarnings("unchecked")
    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        Client elasticClient = null;
        ElasticMutations mutations = null;
        TimingAccessor timing = null;
        if (configuration.getBoolean("elastic")) {
            elasticClient = ElasticClientFactory.create(configuration);
            timing = new TimingAccessor();
            mutations = new ElasticMutations(false, elasticClient, timing);
        }

        String json = configuration.getString("schema");
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(json));
        JSONObject jsonObject = (JSONObject) obj;
        for (Map<String, Object> controller : ((List<Map<String, Object>>) jsonObject.get("controllers"))) {
            String backend = controller.get("backend").toString();
            if (backend.equals("elastic")) {
                controller.put("client", elasticClient);
                controller.put("elasticMutations", mutations);
                controller.put("timing", timing);
            }
            if (controller.get("type").equals("vertex")) {
                VertexController vertex = (VertexController) Class.forName(controller.get("class").toString()).newInstance();
                vertex.init(controller, graph);
                for (String label : ((List<String>) controller.get("labels"))) {
                    addController(vertexControllers, vertex, label);
                }
                if (vertex instanceof EdgeController) {
                    for (String label : ((List<String>) controller.get("edgeLabels"))) {
                        addController(edgeControllers, (EdgeController) vertex, label);
                    }
                }
            } else {
                EdgeController edge = (EdgeController) Class.forName(controller.get("class").toString()).newInstance();
                edge.init(controller, graph);
                for (String label : ((List<String>) controller.get("labels"))) {
                    addController(edgeControllers, edge, label);
                }
            }
        }
    }

    @Override
    public void commit() {
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        throw new NotImplementedException();
    }
}
