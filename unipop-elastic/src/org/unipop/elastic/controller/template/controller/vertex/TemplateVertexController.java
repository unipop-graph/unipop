package org.unipop.elastic.controller.template.controller.vertex;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic.controller.template.helpers.TemplateHelper;
import org.unipop.elastic.controller.template.helpers.TemplateQueryIterator;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateVertexController implements VertexController{

    protected UniGraph graph;
    protected Client client;
    protected ElasticMutations elasticMutations;
    protected int scrollSize;
    protected TimingAccessor timing;
    protected String defaultIndex;
    protected String templateName;
    protected ScriptService.ScriptType type;
    protected Map<String, String> defaultParams;

    public TemplateVertexController(UniGraph graph, Client client, ElasticMutations elasticMutations, int scrollSize,
                                    TimingAccessor timing, String defaultIndex, String templateName, ScriptService.ScriptType type, String... defaultParams) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.scrollSize = scrollSize;
        this.timing = timing;
        this.defaultIndex = defaultIndex;
        this.templateName = templateName;
        this.defaultParams = new HashMap<>();
        this.type = type;
        if (defaultParams.length % 2 == 0)
            for (int i = 0; i < defaultParams.length; i+=2) {
                this.defaultParams.put(defaultParams[i], defaultParams[i+1]);
            }
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
        List<String> paramsList = ((List<String>) conf.getOrDefault("defaultParams", new ArrayList<>()));
        this.defaultParams = new HashMap<>();
        if (paramsList.size() % 2 == 0)
            for (int i = 0; i < paramsList.size(); i+=2) {
                this.defaultParams.put(defaultParams.get(i), paramsList.get(i+1));
            }
    }

    @SuppressWarnings("unchecked")
    protected TemplateVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        return new TemplateVertex(id, label, keyValues, this, graph, elasticMutations, defaultIndex);
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        elasticMutations.refresh(defaultIndex);
        return new TemplateQueryIterator<>(scrollSize,
                predicates.limitHigh,
                client,
                this::createVertex,
                timing,
                templateName,
                TemplateHelper.createTemplateParams(predicates.hasContainers, defaultParams),
                type,
                defaultIndex);
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
