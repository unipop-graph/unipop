package org.unipop.elastic.controller.template.controller.vertex;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic.controller.template.helpers.TemplateHelper;
import org.unipop.elastic.controller.template.helpers.TemplateQueryIterator;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateVertexController implements VertexController {

    protected UniGraph graph;
    protected Client client;
    protected ElasticMutations elasticMutations;
    protected int scrollSize;
    protected TimingAccessor timing;
    protected String defaultIndex;
    protected String templateName;
    protected ScriptService.ScriptType type;
    protected Map<String, String> vertexPath;

    public TemplateVertexController(UniGraph graph, Client client, ElasticMutations elasticMutations, int scrollSize,
                                    TimingAccessor timing, String defaultIndex, String templateName, ScriptService.ScriptType type, Map<String, String> paths) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.scrollSize = scrollSize;
        this.timing = timing;
        this.defaultIndex = defaultIndex;
        this.templateName = templateName;
        this.type = type;
        this.vertexPath = paths;
    }

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        this.graph = graph;
        this.client = ((Client) conf.get("client"));
        this.elasticMutations = ((ElasticMutations) conf.get("elasticMutations"));
        this.scrollSize = Integer.parseInt(conf.getOrDefault("scrollSize", "0").toString());
        this.timing = ((TimingAccessor) conf.get("timing"));
        this.defaultIndex = conf.get("defaultIndex").toString();
        this.templateName = conf.get("templateName").toString();
        this.type = conf.getOrDefault("scriptType", "file").toString().toLowerCase().equals("file") ?
                ScriptService.ScriptType.FILE : ScriptService.ScriptType.INDEXED;
    }

    @Override
    public void addPropertyToVertex(BaseVertex vertex, BaseVertexProperty vertexProperty) {
        throw new NotImplementedException();
    }

    @Override
    public void removePropertyFromVertex(BaseVertex vertex, Property property) {
        throw new NotImplementedException();
    }

    @Override
    public void removeVertex(BaseVertex vertex) {
        throw new NotImplementedException();
    }

    @Override
    public List<BaseElement> vertexProperties(List<BaseVertex> vertices) {
        throw new sun.reflect.generics.reflectiveObjects.NotImplementedException();
    }

    @Override
    public void update(BaseVertex vertex, boolean force) {
        throw new sun.reflect.generics.reflectiveObjects.NotImplementedException();
    }

    @Override
    public String getResource() {
        return defaultIndex;
    }

    @SuppressWarnings("unchecked")
    protected TemplateVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        Map<String, Object> parsedKeyValues = new HashMap<>();
        keyValues.forEach((key, value) -> {
            if (value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                if (map.containsKey("value"))
                    parsedKeyValues.put(key, map.get("value"));

            } else
                parsedKeyValues.put(key, value);
        });
        return new TemplateVertex(id, label, parsedKeyValues, graph.getControllerManager(), graph, elasticMutations, defaultIndex);
    }

    private Object getLabel(Predicates predicates) {
        for (HasContainer hasContainer : predicates.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()))
                return hasContainer.getValue();
        }
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        elasticMutations.refresh(defaultIndex);
        Iterable<BaseVertex> vertices = () -> new TemplateQueryIterator<>(
                predicates.limitHigh,
                client,
                this::createVertex,
                timing,
                templateName,
                TemplateHelper.createTemplateParams(predicates),
                type,
                vertexPath,
                defaultIndex);

        Object labels = getLabel(predicates);
        Set<String> labelsSet = new HashSet<>();
        if (labels != null) {
            if (labels instanceof Iterable) {
                ((Iterable<String>) labels).forEach(labelsSet::add);
            } else
                labelsSet.add(labels.toString());
        }

        return StreamSupport.stream(vertices.spliterator(), false).filter(vertex -> labelsSet.isEmpty() || labelsSet.contains(vertex.label())).collect(Collectors.toList()).iterator();
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        Predicates p = new Predicates();
        p.limitHigh = 1;
        p.hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(vertexId)));
        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.eq(vertexLabel)));
        return vertices(p).next();
    }

    @Override
    public long vertexCount(Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        client.close();
    }
}
