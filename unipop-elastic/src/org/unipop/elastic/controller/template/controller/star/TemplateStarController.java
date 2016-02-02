package org.unipop.elastic.controller.template.controller.star;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.template.controller.star.inneredge.TemplateInnerEdgeController;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertex;
import org.unipop.elastic.controller.template.controller.vertex.TemplateVertexController;
import org.unipop.elastic.controller.template.helpers.TemplateQueryIterator;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateStarController extends TemplateVertexController implements EdgeController {
    private Set<TemplateInnerEdgeController> edgeControllers;

    public TemplateStarController(UniGraph graph, Client client, ElasticMutations elasticMutations, int scrollSize, TimingAccessor timing, String defaultIndex, String templateName, ScriptService.ScriptType type, Set<TemplateInnerEdgeController> edgeControllers, String... defaultParams) {
        super(graph, client, elasticMutations, scrollSize, timing, defaultIndex, templateName, type, defaultParams);
        this.edgeControllers = edgeControllers;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        super.init(conf, graph);
        for (Map<String, Object> edge : ((List<Map<String, Object>>) conf.get("edges"))) {
            TemplateInnerEdgeController innerEdge = ((TemplateInnerEdgeController) Class.forName(edge.get("class").toString()).newInstance());
            innerEdge.init(edge);
            edgeControllers.add(innerEdge);
        }
    }

    @Override
    protected TemplateVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        TemplateStarVertex star = new TemplateStarVertex(id, label, keyValues, this, graph, elasticMutations, defaultIndex);
        edgeControllers.forEach(edgeController -> edgeController.parseEdges(star, keyValues));
        return star;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        return null;
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Set<BaseEdge> results = new HashSet<>();
        List<String> labels = Arrays.asList(edgeLabels);

        for (Vertex vertex : vertices) {
            if (vertex instanceof TemplateStarVertex)
                results.addAll(((TemplateStarVertex) vertex).getInnerEdges(direction, labels, predicates));
        }

        Map<String, Object> params = new HashMap<>();

        for (TemplateInnerEdgeController controller : edgeControllers) {
            controller.getParams(vertices, direction, edgeLabels, predicates).forEach(params::put);
        }

        if (!params.isEmpty()) {
            new TemplateQueryIterator<BaseVertex>(scrollSize, predicates.limitHigh, client, this::createVertex,timing,
                    templateName,params,type, defaultIndex)
                        .forEachRemaining(vertex-> ((TemplateStarVertex) vertex).getInnerEdges(direction.opposite(),
                                Arrays.asList(edgeLabels),predicates).forEach(results::add));
        }

        return results.iterator();
    }

    @Override
    public long edgeCount(Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return null;
    }
}
