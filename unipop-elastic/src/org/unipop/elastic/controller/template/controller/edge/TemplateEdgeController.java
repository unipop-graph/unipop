package org.unipop.elastic.controller.template.controller.edge;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.template.helpers.TemplateHelper;
import org.unipop.elastic.controller.template.helpers.TemplateQueryIterator;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateEdgeController implements EdgeController {
    private UniGraph graph;
    private Client client;
    private ElasticMutations elasticMutations;
    private String indexName;
    private int scrollSize;
    private TimingAccessor timing;
    private String templateName;
    private ScriptService.ScriptType type;
    private Map<String, String> defaultParams;

    public TemplateEdgeController(UniGraph graph, Client client, ElasticMutations elasticMutations, String indexName, int scrollSize, TimingAccessor timing,
                                  String templateName, ScriptService.ScriptType type, String... defaultParams) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.timing = timing;
        this.templateName = templateName;
        this.type = type;

        this.defaultParams = new HashMap<>();
        if (defaultParams.length % 2 == 0)
            for (int i = 0; i < defaultParams.length; i += 2) {
                this.defaultParams.put(defaultParams[i], defaultParams[i + 1]);
            }
    }

    private BaseEdge createEdge(Object id, String label, Map<String, Object> fields) {
        BaseVertex outV = this.graph.getControllerManager().vertex(Direction.OUT, fields.get("outId"), fields.get("outLabel").toString());
        BaseVertex inV = this.graph.getControllerManager().vertex(Direction.IN, fields.get("inId"), fields.get("inLabel").toString());
        BaseEdge edge = new TemplateEdge(id, label, fields, outV, inV, this, graph, elasticMutations, indexName);
        return edge;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        elasticMutations.refresh();
        return new TemplateQueryIterator<>(scrollSize,
                predicates.limitHigh,
                client,
                this::createEdge,
                timing,
                templateName,
                TemplateHelper.createTemplateParams(predicates.hasContainers, defaultParams),
                type,
                indexName);
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Set<Object> ids = Stream.of(vertices).map(Element::id).collect(Collectors.toSet());
        Set<String> labels = Stream.of(vertices).map(Element::label).collect(Collectors.toSet());
        if (direction.equals(Direction.BOTH) || direction.equals(Direction.OUT)) {
            predicates.hasContainers.add(new HasContainer("outId", P.within(ids)));
            predicates.hasContainers.add(new HasContainer("outLabel", P.within(labels)));
        }
        if (direction.equals(Direction.BOTH) || direction.equals(Direction.IN)){
            predicates.hasContainers.add(new HasContainer("inId", P.within(ids)));
            predicates.hasContainers.add(new HasContainer("inLabel", P.within(labels)));
        }
        return new TemplateQueryIterator<>(scrollSize,
                predicates.limitHigh,
                client,
                this::createEdge,
                timing,
                templateName,
                TemplateHelper.createTemplateParams(predicates.hasContainers, defaultParams),
                type,
                indexName);
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
        TemplateEdge templateEdge = new TemplateEdge(edgeId, label, properties, outV, inV, this, graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(templateEdge, indexName, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(templateEdge.id());
        }
        return templateEdge;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {

    }

    @Override
    public void close() {
        client.close();
    }
}
