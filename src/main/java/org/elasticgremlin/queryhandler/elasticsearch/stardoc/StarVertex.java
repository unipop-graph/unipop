package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.elasticsearch.vertexdoc.DocVertex;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class StarVertex extends DocVertex {
    private Set<InnerEdge> innerEdges;

    public StarVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph, LazyGetter lazyGetter, ElasticMutations elasticMutations, String indexName) {
        super(id, label, keyValues, graph, lazyGetter, elasticMutations, indexName);
        innerEdges = new HashSet<>();
    }




    @Override
    public Iterator<Edge> cachedEdges(Direction direction, String[] edgeLabels, Predicates predicates) {
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
        else return null;
    }
}
