package org.unipop.elastic.controller.star;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.edge.ElasticEdge;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class StarController extends ElasticVertexController implements EdgeController {

    private ArrayList<EdgeMapping> edgeMappings;


    public StarController(UniGraph graph, Client client, ElasticMutations elasticMutations, String defaultIndex,
                          int scrollSize, boolean refresh, TimingAccessor timing, EdgeMapping... edgeMappings) {
        super(graph, client, elasticMutations, defaultIndex, scrollSize, timing);
        this.edgeMappings = new ArrayList<>();
        Collections.addAll(this.edgeMappings, edgeMappings);
    }

    @Override
    protected BaseVertex createVertex(SearchHit hit) {
        ElasticStarVertex vertex = new ElasticStarVertex(hit.id(), hit.type(), null, graph, new LazyGetter(client, timing), this, elasticMutations, getDefaultIndex());
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        vertex.createEdges(edgeMappings, hit.getSource());
        return vertex;
    }

    @Override
    protected ElasticVertex createVertex(Object id, String label, Map<String, Object> keyValues, LazyGetter lazyGetter) {
        return new ElasticStarVertex(id, label, keyValues, graph, lazyGetter, this, elasticMutations, getDefaultIndex());
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        Predicates predicates = new Predicates();
        predicates.hasContainers.add(new HasContainer("~label", P.eq(vertexLabel)));
        predicates.hasContainers.add(new HasContainer("~id", P.eq(vertexId)));
        return vertices(predicates, new MutableMetrics("fromEdge", "fromEdge")).next();
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        ArrayList<BaseEdge> edges = new ArrayList<>();
        vertices(new Predicates(), null).forEachRemaining(baseVertex ->
                        baseVertex.cachedEdges(Direction.OUT, new String[0], predicates).forEachRemaining(edge -> edges.add(((BaseEdge) edge)))
        );
        return edges.iterator();
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        ArrayList<BaseEdge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            for (Vertex vertex : vertices) {
                ((ElasticStarVertex) vertex).cachedEdges(Direction.OUT, edgeLabels, predicates).forEachRemaining(edge -> edges.add(((InnerEdge) edge)));
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            BoolFilterBuilder bool = FilterBuilders.boolFilter();
            Object[] ids = new Object[vertices.length];
            for (int i = 0; i < vertices.length; i++) {
                ids[i] = vertices[i].id();
            }
            if (edgeLabels.length > 0) {
                for (String label : edgeLabels) {
                    Predicates p = new Predicates();
                    p.hasContainers.add(new HasContainer(label + "." + ElasticEdge.InId, P.within(ids)));
                    NestedFilterBuilder filter = FilterBuilders.nestedFilter(label, ElasticHelper.createFilterBuilder(p.hasContainers));
                    bool.should(filter);
                }
            } else {
                for (EdgeMapping mapping : this.edgeMappings) {
                    Predicates p = new Predicates();
                    p.hasContainers.add(new HasContainer(mapping.getLabel() + "." + ElasticEdge.InId, P.within(ids)));
                    NestedFilterBuilder filter = FilterBuilders.nestedFilter(mapping.getExternalVertexField(), ElasticHelper.createFilterBuilder(p.hasContainers));
                    bool.should(filter);
                }
            }

            QueryIterator<Vertex> edgesVertices =
                    new QueryIterator<>(bool,
                            (int) predicates.limitLow,
                            scrollSize, predicates.limitHigh,
                            client,
                            this::createVertex,
                            timing,
                            getDefaultIndex());
            edgesVertices.forEachRemaining(vertex -> ((ElasticStarVertex) vertex).cachedEdges(Direction.OUT, edgeLabels, predicates).forEachRemaining(edge -> edges.add(((InnerEdge) edge))));
        }
        return edges.iterator();
    }

    public ElasticMutations getElasticMutations(){
        return elasticMutations;
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Map<String, Object> properties) {
        EdgeMapping mapping = getEdgeMapping(label, Direction.OUT);
        if (mapping == null) {
            mapping = new NestedEdgeMapping(label, inV.label(), Direction.OUT, label);
            edgeMappings.add(mapping);
            Map<String, Object> propertiesMap = new HashMap<>();
            Map<String, Object> nested = new HashMap<>();
            nested.put("type", "nested");
            propertiesMap.put(label, propertiesMap);
            try {
                XContentBuilder nestedMapping = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject(outV.label())
                        .startObject("properties")
                        .startObject(label)
                        .field("type", "nested")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
                client.admin().indices().preparePutMapping(getDefaultIndex()).setType(outV.label()).setSource(nestedMapping).execute().actionGet();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        BaseEdge edge = ((ElasticStarVertex) outV).addInnerEdge(mapping, edgeId, inV, properties);
        try {
            elasticMutations.updateElement(outV, getDefaultIndex(), null, true);
            elasticMutations.refresh();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return edge;
    }

    public static boolean contains(String[] edgeLabels, String label) {
        for (String edgeLabel : edgeLabels)
            if (edgeLabel.equals(label)) return true;
        return false;
    }

    private EdgeMapping getEdgeMapping(String label, Direction direction) {
        for (EdgeMapping mapping : edgeMappings) {
            if (mapping.getLabel().equals(label) && mapping.getDirection().equals(direction)) {
                return mapping;
            }
        }
        return null;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(ElasticEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, timing, getDefaultIndex());
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        BaseVertex vertex = super.addVertex(id, label, properties);
//        elasticMutations.refresh();
        return vertex;
    }
}
