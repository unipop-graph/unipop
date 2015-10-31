package org.unipop.elastic.controller.star;

import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.elastic.controller.vertex.ElasticVertexController;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.LazyGetter;
import org.unipop.elastic.controller.vertex.ElasticVertex;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.unipop.structure.*;

import java.util.*;

public class ElasticStarVertex extends ElasticVertex {
    private Set<InnerEdge> innerEdges;
    public String indexName;

    public ElasticStarVertex(final Object id,
                             final String label,
                             Map<String, Object> keyValues,
                             UniGraph graph,
                             LazyGetter lazyGetter,
                             ElasticVertexController controller,
                             ElasticMutations elasticMutations,
                             String indexName) {
        super(id, label, keyValues, controller, graph, lazyGetter, elasticMutations, indexName);
        this.indexName = indexName;
        innerEdges = new HashSet<>();
    }

    public void createEdges(List<EdgeMapping> mappings,
                            Map<String, Object> source) {
        mappings.forEach(edgeMapping -> addEdges(edgeMapping, source));
    }

    private void addEdges(EdgeMapping edgeMapping, Map<String, Object> source) {
        Iterable<Object> vertices = edgeMapping.getExternalVertexId(source);
        vertices.forEach(externalId -> {
            InnerEdge edge = new InnerEdge(id.toString() + label() + externalId.toString(),
                    edgeMapping,
                    this,
                    getController().fromEdge(Direction.OUT, externalId, edgeMapping.getExternalVertexLabel()),
                    edgeMapping.getProperties(source, externalId),
                    ((EdgeController) getController()),
                    graph);
            innerEdges.add(edge);
        });
    }


    @Override
    public Iterator<BaseEdge> cachedEdges(Direction direction, String[] edgeLabels, Predicates predicates) {
        ArrayList<BaseEdge> edges = new ArrayList<>();
        innerEdges.forEach(edge -> {
            EdgeMapping mapping = edge.getMapping();
            if (mapping.getDirection().equals(direction) &&
                    (edgeLabels.length == 0 || StarController.contains(edgeLabels, mapping.getLabel()))) {

                // Test predicates on inner edge
                boolean passed = true;
                for (HasContainer hasContainer : predicates.hasContainers) {
                    if (hasContainer.getKey().equals(T.id.getAccessor())) {
                        if(hasContainer.getValue().getClass().isArray())
                            for (Object id : ((Object[]) hasContainer.getValue())) {
                                if (!id.equals(edge.id()))
                                    passed = false;
                            }
                        else if (!hasContainer.getValue().equals(edge.id())) passed = false;
                    } else if (!hasContainer.test(edge)) {
                        passed = false;
                    }
                }
                if (passed) {
                    edges.add(edge);
                }
            }
        });

        if (edges.size() > 0) return edges.iterator();
        else return new ArrayList<BaseEdge>().iterator();
    }

    public BaseEdge addInnerEdge(EdgeMapping mapping, Object edgeId, Vertex inV, Map<String, Object> properties) {
        if (edgeId == null)
            edgeId = id.toString() + inV.id();
        InnerEdge edge = new InnerEdge(edgeId, mapping, this, inV, properties, ((EdgeController) getController()), graph);
        innerEdges.add(edge);
        return edge;
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = super.allFields();
        Map<String, ArrayList<Map<String, Object>>> edges = new HashMap<>();
        innerEdges.forEach(edge -> {
            if (edges.containsKey(edge.label())) {
                edges.get(edge.label()).add(edge.getMap());
            } else {
                edges.put(edge.label(), new ArrayList<Map<String, Object>>() {{
                    add(edge.getMap());
                }});
            }
        });
        edges.forEach(map::put);
        return map;
    }
}
