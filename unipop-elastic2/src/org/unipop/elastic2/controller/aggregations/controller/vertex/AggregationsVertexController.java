package org.unipop.elastic2.controller.aggregations.controller.vertex;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.unipop.query.UniQuery;
import org.unipop.elastic2.controller.aggregations.controller.edge.AggregationsEdge;
import org.unipop.elastic2.controller.aggregations.helpers.AggregationsHelper;
import org.unipop.elastic2.controller.aggregations.helpers.AggregationsQueryIterator;
import org.unipop.elastic2.helpers.ElasticMutations;
import org.unipop.elastic2.helpers.TimingAccessor;
import org.unipop.structure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class AggregationsVertexController implements VertexQueryController, EdgeQueryController {

    protected UniGraph graph;
    protected Client client;
    protected ElasticMutations elasticMutations;
    protected int scrollSize;
    protected TimingAccessor timing;
    protected String defaultIndex;
    protected String templateName;
    protected ScriptService.ScriptType type;
    protected Map<String, String> vertexPath;

    public AggregationsVertexController(UniGraph graph, Client client, ElasticMutations elasticMutations, int scrollSize,
                                        TimingAccessor timing, String defaultIndex, String templateName,
                                        ScriptService.ScriptType type, Map<String, String> vertexPaths) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.scrollSize = scrollSize;
        this.timing = timing;
        this.defaultIndex = defaultIndex;
        this.templateName = templateName;
        this.type = type;
        this.vertexPath = vertexPaths;
    }

    @Override
    public Iterator<UniEdge> edges(UniQuery uniQuery) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<UniEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        List<UniEdge> edges = new ArrayList<>();
        for (Vertex vertex : vertices) {
            ((AggregationsVertex) vertex).getInnerEdges(direction, Arrays.asList(edgeLabels), uniQuery).forEach(edge -> edges.add(((AggregationsEdge) edge)));
        }
        return edges.iterator();
    }

    @Override
    public long edgeCount(UniQuery uniQuery) {
        return 0;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery) {
        return 0;
    }

    @Override
    public Map<String, Object> edgeGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        return null;
    }

    @Override
    public UniEdge addEdge(Object edgeId, String label, UniVertex outV, UniVertex inV, Map<String, Object> properties) {
        return null;
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
        vertexPath = new HashMap<>();
        ((ArrayList<Map<String, Object>>) conf.get("paths")).forEach(path -> vertexPath.put(path.get("path").toString(), path.get("label").toString()));
        this.type = conf.getOrDefault("scriptType", "file").toString().toLowerCase().equals("file") ?
                ScriptService.ScriptType.FILE : ScriptService.ScriptType.INDEXED;
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public void addPropertyToVertex(UniVertex vertex, UniVertexProperty vertexProperty) {
        throw new NotImplementedException();
    }

    @Override
    public void removePropertyFromVertex(UniVertex vertex, Property property) {
        throw new NotImplementedException();
    }

    @Override
    public void removeVertex(UniVertex vertex) {
        throw new NotImplementedException();
    }

    @Override
    public List<UniElement> vertexProperties(Iterator<UniVertex> vertices) {
        throw new NotImplementedException();
    }

    @SuppressWarnings("unchecked")
    protected AggregationsVertex createVertex(Object id, String label, Map<String, Object> keyValues) {
        Map<String, Object> parsedKeyValues = new HashMap<>();
        AggregationsVertex vertex = new AggregationsVertex(id, label, parsedKeyValues, graph.getControllerManager(), graph, elasticMutations, defaultIndex);
        keyValues.forEach((key, value) -> {
            if (value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                if (map.containsKey("value"))
                    parsedKeyValues.put(key, map.get("value"));
                else if (map.containsKey("buckets")) {
                    ((JSONArray) map.get("buckets")).forEach(
                            bucket -> {
                                JSONObject jsonObject = (JSONObject) bucket;
                                Object innerId = jsonObject.get("key");
                                jsonObject.remove("key");
                                Map<String, Object> edgeProperties = new HashMap<>();
                                jsonObject.forEach((edgeKey, edgeValue) ->{
                                    if (edgeValue instanceof JSONObject)
                                        edgeProperties.put(edgeKey.toString(), ((JSONObject) edgeValue).get("value"));
                                    else
                                        edgeProperties.put(edgeKey.toString(), edgeValue);
                                });
                                AggregationsVertex innerVertex = new AggregationsVertex(innerId, "template", null, graph.getControllerManager(), graph, elasticMutations, defaultIndex);
                                AggregationsEdge innerEdge = new AggregationsEdge(innerId.hashCode(), "templateEdge", edgeProperties, vertex, innerVertex, this, graph);
                                vertex.addInnerEdge(innerEdge);
                            }
                    );
                }

            } else
                parsedKeyValues.put(key, value);
        });

        parsedKeyValues.forEach(vertex::addPropertyLocal);

        return vertex;
    }

    private Object getLabel(UniQuery uniQuery) {
        for (HasContainer hasContainer : uniQuery.hasContainers) {
            if (hasContainer.getKey().equals(T.label.getAccessor()))
                return hasContainer.getValue();
        }
        return null;
    }

    @Override
    public Iterator<UniVertex> vertices(UniQuery uniQuery) {
        elasticMutations.refresh(defaultIndex);
        Iterable<UniVertex> vertices = () -> new AggregationsQueryIterator<>(
                uniQuery.limitHigh,
                client,
                this::createVertex,
                timing,
                templateName,
                AggregationsHelper.createTemplateParams(uniQuery),
                type,
                vertexPath,
                defaultIndex);

        Object labels = getLabel(uniQuery);
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
    public UniVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        UniQuery p = new UniQuery();
        p.limitHigh = 1;
        p.hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(vertexId)));
        p.hasContainers.add(new HasContainer(T.label.getAccessor(), P.eq(vertexLabel)));
        return vertices(p).next();
    }

    @Override
    public long vertexCount(UniQuery uniQuery) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> vertexGroupBy(UniQuery uniQuery, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public UniVertex addVertex(Object id, String label, Map<String, Object> properties) {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        client.close();
    }
}
