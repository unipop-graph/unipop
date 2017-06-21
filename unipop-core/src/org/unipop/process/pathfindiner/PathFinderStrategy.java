package org.unipop.process.pathfindiner;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.unipop.query.controller.SimpleController;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Created by sbarzilay on 6/21/17.
 */
public class PathFinderStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private TinkerGraph schemaGraph;

    private void init(UniGraph uniGraph) {
        this.schemaGraph = TinkerGraph.open();
        uniGraph.getControllerManager().getControllers().stream().map(controller -> ((SimpleController) controller))
                .forEach(controller -> {
                    controller.getVertexSchemas().forEach(schema -> {
                        schemaGraph.addVertex(T.id,
                                schema.getPropertySchema("schemaId").toProperties(Collections.emptyMap()).get("schemaId"),
                                "schema",
                                schema);
                    });
                });
        System.out.println("test");
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        Optional<Graph> graph = traversal.getGraph();
        if (!graph.isPresent())
            return;
        init((UniGraph) graph.get());
    }
}
