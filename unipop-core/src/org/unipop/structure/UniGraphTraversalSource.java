package org.unipop.structure;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Created by sbarzilay on 10/2/16.
 */
public class UniGraphTraversalSource extends GraphTraversalSource {
    protected UniGraph graph;
    protected TraversalStrategies strategies;

    public UniGraphTraversalSource(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
        this.graph = (UniGraph) graph;
        this.strategies = traversalStrategies;
    }

    @Override
    public GraphTraversalSource withBulk(final boolean useBulk) {
        if (!useBulk) {
            final UniGraphTraversalSource clone = (UniGraphTraversalSource) this.clone();
            RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.ONE_BULK);
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public GraphTraversalSource withPath() {
        final UniGraphTraversalSource clone = (UniGraphTraversalSource) this.clone();
        RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.PATH);
        return clone;
    }

    @Override
    public GraphTraversalSource clone() {
        final UniGraphTraversalSource clone = (UniGraphTraversalSource) super.clone();
        clone.strategies = this.strategies.clone();
        return clone;
    }

//    @Override
//    public GraphTraversalSource withStrategies(TraversalStrategy... traversalStrategies) {
//        GraphTraversalSource clone = this.clone();
//        clone.getStrategies().addStrategies(traversalStrategies);
//        return clone;
//    }

    private <S> GraphTraversal.Admin<S, S> generateTraversal() {
        final org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Admin<S, S> traversal = new UniGraphTraversal<>(this.graph);
        traversal.setStrategies(this.strategies);
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E(Object... edgesIds) {
        final GraphTraversal.Admin<Edge, Edge> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, edgesIds));
    }

    public GraphTraversal<Vertex, Vertex> V(Object... vertexIds) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        return (GraphTraversal<S, S>) this.generateTraversal().inject(starts);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> addV(String label) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new AddVertexStartStep(traversal, label));
    }

    @Override
    public GraphTraversal<Vertex, Vertex> addV() {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new AddVertexStartStep(traversal, null));
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public Graph getGraph() {
        return this.graph;
    }
}
