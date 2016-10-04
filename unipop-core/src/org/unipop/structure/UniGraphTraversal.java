package org.unipop.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.unipop.process.local.UniGraphProjectStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 10/2/16.
 */
public class UniGraphTraversal<S,E> extends DefaultGraphTraversal<S,E> {

    public UniGraphTraversal(UniGraph graph) {
        super(graph);
    }

    @Override
    public <E2> GraphTraversal<S, Map<String, E2>> project(String projectKey, String... otherProjectKeys) {
        final String[] projectKeys = new String[otherProjectKeys.length + 1];
        projectKeys[0] = projectKey;
        System.arraycopy(otherProjectKeys, 0, projectKeys, 1, otherProjectKeys.length);

        List<LocalQuery.LocalController> localControllers = ((UniGraph) this.getGraph().get()).getControllerManager()
                .getControllers(LocalQuery.LocalController.class);

        List<SearchVertexQuery.SearchVertexController> nonLocalControllers = ((UniGraph) this.getGraph().get()).getControllerManager().getControllers(SearchVertexQuery.SearchVertexController.class)
                .stream().filter(controller -> !localControllers.contains(controller)).collect(Collectors.toList());

        return this.asAdmin().addStep(new UniGraphProjectStep<S, E2>(this.asAdmin(), (UniGraph) this.getGraph().get(), projectKeys, localControllers, nonLocalControllers));
    }
}
