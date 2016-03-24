package org.unipop.elastic.controllerprovider;

import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.structure.manager.ControllerProvider;
import org.unipop.controller.VertexController;
import org.unipop.controller.virtualvertex.VirtualVertexController;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertexController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.*;

import java.util.*;

/**
 * Created by sbarzilay on 2/12/16.
 */
public class ImdbControllerProvider extends ControllerProvider {

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        Client client = ElasticClientFactory.create(configuration);
        String indexName = "imdb";
        ElasticHelper.createIndex(indexName, client);

        TimingAccessor timing = new TimingAccessor();
        ElasticMutations elasticMutations = new ElasticMutations(false, client, timing);

        HashMap<Object, Object> vertexControllers = new HashMap<>();
        Map<String,String> paths = new HashMap<>();
        paths.put("hits.hits", "");
        paths.put("aggregations.all.genre.buckets", "genre");
        VertexController movie = new TemplateVertexController(graph, client, elasticMutations, 0, timing, indexName, "dyn_template", ScriptService.ScriptType.FILE, paths);
        //TODO: Schema
//        vertexControllers.put("movie", movie);
//        vertexControllers.put("genre", movie);
        this.addController(movie);

        VertexController virtual = new VirtualVertexController(graph, "actor");
        //TODO: Schema
//        vertexControllers.put("actor", virtual);
//        vertexControllers.put("director", virtual);
        this.addController(virtual);
    }
}
