package org.elasticgremlin.queryhandler.stardoc;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.ElasticMutations;
import org.elasticgremlin.elasticsearch.LazyGetter;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class StarVertex extends BaseVertex {
    private static final int EXTERNAL_VERTEX_BULK = 500;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final EdgeMapping[] edgeMappings;
    private LazyGetter lazyGetter;
    private Set<InnerEdge> innerEdges;

    public StarVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph,
                      LazyGetter lazyGetter, ElasticMutations elasticMutations, String indexName,
                      EdgeMapping[] edgeMappings) {
        super(id, label, graph, keyValues);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.edgeMappings = edgeMappings;
        innerEdges = new HashSet<>();
        if(lazyGetter != null) {
            this.lazyGetter = lazyGetter;
            lazyGetter.register(this, this.indexName);
        }
    }

    @Override
    public String label() {
        if(this.label == null && lazyGetter != null) lazyGetter.execute();
        return super.label();
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if(lazyGetter != null) lazyGetter.execute();
        return super.property(key);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void innerRemove() {
        elasticMutations.deleteElement(this, indexName, null);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if(lazyGetter != null) lazyGetter.execute();
        return super.properties(propertyKeys);
    }

    @Override
    public void applyLazyFields(MultiGetItemResponse response) {
        GetResponse getResponse = response.getResponse();
        if(getResponse.isSourceEmpty()) return;
        setFields(getResponse.getSource());
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        ArrayList<Edge> edges = new ArrayList<>();
        innerEdges.forEach(edge -> {
            EdgeMapping mapping = edge.getMapping();
            if(mapping.getDirection().equals(direction) &&
                (edgeLabels.length == 0 || StarHandler.contains(edgeLabels, mapping.getLabel()))) {

                // Test predicates on inner edge
                boolean passed = true;
                for (HasContainer hasContainer : predicates.hasContainers) {
                    if (!hasContainer.test(edge)) {
                        passed = false;
                    }
                }
                if (passed) {
                    edges.add(edge);
                }
            }
        });

        if(edges.size() > 0) return edges.iterator();

        ArrayList<String> externalEdgeLabels = new ArrayList<>();
        for (String label : edgeLabels) {
            boolean internal = false;
            for (EdgeMapping mapping : edgeMappings) {
                if (label.equals(mapping.getLabel())) {
                    // This label is internal
                    internal = true;
                }
            }
            if (!internal) {
                externalEdgeLabels.add(label);
            }
        }
        if (!externalEdgeLabels.isEmpty()) {
            return super.edges(direction, externalEdgeLabels.toArray(new String[externalEdgeLabels.size()]), predicates);
        }
        else {
            return new ArrayList<Edge>(0).iterator();
        }
    }

    public void setFields(Map<String, Object> entries){
        entries.entrySet().forEach(field -> {
            if(field.getValue() != null) addPropertyLocal(field.getKey(), field.getValue());
        });
        for(EdgeMapping mapping : edgeMappings){
            Object externalVertexId = mapping.getExternalVertexId(entries);
            if(externalVertexId == null) continue;
            Vertex externalVertex = graph.getQueryHandler().vertex(externalVertexId,
                    mapping.getExternalVertexLabel(), null, mapping.getDirection().opposite());
            Object[] keyValues = mapping.getProperties(entries);
            InnerEdge innerEdge = new InnerEdge(mapping, this, externalVertex,
                    keyValues,
                    graph);
            this.innerEdges.add(innerEdge);
        }
    }

    /**
     * Set vertices to be siblings of each other,
     * and do the same to the vertices' external siblings
     * @param siblings Set the external vertices of each of these vertices to be siblings
     */
    public static void setVertexAndExternalSiblings(List<Vertex> siblings) {
        BaseVertex.setVertexSiblings(siblings);

        // Make sure all vertices are of class StarVertex
        List<Vertex> vertexSiblings = new ArrayList<>();
        for (Vertex element : siblings) {
            if (!StarVertex.class.isAssignableFrom(element.getClass())) continue;
            vertexSiblings.add(element);
        }

        List<Vertex> externalVertexSiblings = new ArrayList<>();
        for (Vertex vertex : vertexSiblings) {
            // Add all external vertices to list
            StarVertex starVertex = (StarVertex) vertex;
            for (InnerEdge innerEdge : starVertex.innerEdges) {
                switch (innerEdge.getMapping().getDirection()) {
                    case IN:
                        externalVertexSiblings.add(innerEdge.outVertex());
                        break;
                    case OUT:
                        externalVertexSiblings.add(innerEdge.inVertex());
                        break;
                }
            }
            if (externalVertexSiblings.size() >= EXTERNAL_VERTEX_BULK) {
                BaseVertex.setVertexSiblings(externalVertexSiblings);
                externalVertexSiblings = new ArrayList<>();
            }
        }
        BaseVertex.setVertexSiblings(externalVertexSiblings);
    }
}
