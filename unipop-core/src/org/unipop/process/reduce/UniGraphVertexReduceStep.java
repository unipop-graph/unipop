package org.unipop.process.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.process.UniQueryStep;
import org.unipop.query.StepDescriptor;
import org.unipop.query.UniQuery;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.aggregation.ReduceVertexQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/31/16.
 */
public class UniGraphVertexReduceStep<S, E> extends AbstractStep<S, E> implements UniQueryStep<Vertex>, Profiling {
    private final BinaryOperator<E> biOperator;
    private final Traversal.Admin<S, E> reduceTraversal;
    private final Supplier<E> seedSupplier;
    private Direction direction;
    private StepDescriptor stepDescriptor;
    private List<ReduceVertexQuery.ReduceVertexController> reduceControllers;
    private PredicatesHolder predicatesHolder;
    private Set<String> propertyKeys;
    private int limit;
    private E seed = null;
    private boolean returnsVertex;
    private boolean done;
    private String reduceOn;
    private ReduceQuery.ReduceOperator op;

    public UniGraphVertexReduceStep(boolean returnsVertex, Direction direction, PredicatesHolder predicates, Set<String> propertyKeys, String reduceOn, int limit, List<ReduceVertexQuery.ReduceVertexController> reduceControllers,
                                    Class returnType, BinaryOperator<E> biOperator, Supplier<E> seedSuplier, ReduceQuery.ReduceOperator op, Traversal.Admin<S, E> reduceTraversal, Traversal.Admin traversal, UniGraph graph) {
        super(traversal);
        this.propertyKeys = propertyKeys;
        this.op = op;
        this.stepDescriptor = new StepDescriptor(this);
        this.predicatesHolder = predicates;
        this.limit = limit;
        this.biOperator = biOperator;
        this.reduceControllers = reduceControllers;
        this.reduceTraversal = reduceTraversal;
        this.seedSupplier = seedSuplier;
        this.returnsVertex = returnsVertex;
        this.done = false;
        this.reduceOn = reduceOn;
        this.direction = direction;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (done)
            throw FastNoSuchElementException.instance();
        List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
        this.starts.forEachRemaining(start -> vertices.add((Traverser.Admin<Vertex>) start));
        ReduceVertexQuery query = (ReduceVertexQuery) getQuery(vertices);

        List<E> eList = reduceControllers.stream()
                .map(reduceController -> (E)reduceController.<E>reduce(query))
                .collect(Collectors.toList());
        for (E temp: eList) {
            if (temp != null) {
                if (seed == null) this.seed = seedSupplier.get();
                Iterator iter = ((Iterator) temp);
                while (iter.hasNext())
                    this.seed = this.biOperator.apply(this.seed, (E) iter.next());
            }
        }
        vertices.forEach(vertex -> reduceTraversal.addStart((Traverser.Admin<S>) vertex));
        while (reduceTraversal.hasNext()) {
            if (this.seed == null) this.seed = seedSupplier.get();
            E next = reduceTraversal.next();
            if (next instanceof MeanGlobalStep.MeanNumber){
                Double.isNaN(((MeanGlobalStep.MeanNumber) next).doubleValue());
                this.seed = this.biOperator.apply(this.seed, next);
            }
            else{
                if (!(next instanceof Double) || !Double.isNaN((Double) next))
                    this.seed = this.biOperator.apply(this.seed, next);
            }
        }
        if (this.seed == null)
            throw FastNoSuchElementException.instance();
        E e = ((ReducingBarrierStep<S, E>) reduceTraversal.getEndStep()).generateFinalResult(this.seed);
        final Traverser.Admin<E> traverser = this.getTraversal().getTraverserGenerator().generate(e, (Step<E, E>) this, 1l);
        this.seed = null;
        this.done = true;
        return traverser;
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        this.stepDescriptor = new StepDescriptor(this, metrics);
    }

    @Override
    public UniQuery getQuery(List<Traverser.Admin<Vertex>> traversers) {
        ReduceVertexQuery query = new ReduceVertexQuery(returnsVertex, traversers.stream().map(Traverser::get).collect(Collectors.toList()),
                direction, predicatesHolder,
                propertyKeys, reduceOn, op, limit, stepDescriptor);
        return query;
    }

    @Override
    public boolean hasControllers() {
        return reduceControllers.size() > 0;
    }

    @Override
    public void addControllers(List<UniQueryController> controllers) {
        controllers.forEach(controller -> {
            this.reduceControllers.add(((ReduceVertexQuery.ReduceVertexController) controller));
        });
    }
}
