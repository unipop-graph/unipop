package org.unipop.process.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.NumberHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.unipop.query.StepDescriptor;
import org.unipop.query.aggregation.ReduceQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/29/16.
 */
public class UniGraphReduceStep<S, E> extends AbstractStep<S, E> implements Profiling {
    private final BinaryOperator<E> biOperator;
    private final Traversal.Admin<S,E> reduceTraversal;
    private final Supplier<E> seedSupplier;
    private StepDescriptor stepDescriptor;
    private List<ReduceQuery.ReduceController> reduceControllers;
    private PredicatesHolder predicatesHolder;
    private Set<String> propertyKeys;
    private int limit;
    private E seed = null;
    private Class returnType;
    private boolean done;
    private String reduceOn;
    private ReduceQuery.ReduceOperator op;


    public UniGraphReduceStep(PredicatesHolder predicates, Set<String> propertyKeys, String reduceOn, int limit, List<ReduceQuery.ReduceController> reduceControllers,
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
        this.returnType = returnType;
        this.done = false;
        this.reduceOn = reduceOn;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        ReduceQuery reduceQuery = new ReduceQuery(predicatesHolder, propertyKeys, reduceOn, op, returnType, limit, stepDescriptor);
        if (done)
            throw FastNoSuchElementException.instance();
        List<E> eList = reduceControllers.stream()
                .map(reduceController -> (E)reduceController.<E>reduce(reduceQuery))
                .collect(Collectors.toList());
        for (E temp: eList) {
            if (temp != null) {
                if (seed == null) this.seed = seedSupplier.get();
                Iterator iter = ((Iterator) temp);
                while (iter.hasNext())
                    this.seed = this.biOperator.apply(this.seed, (E) iter.next());
            }
        }
        while (reduceTraversal.hasNext()) {
            if (this.seed == null) this.seed = seedSupplier.get();
            E next = reduceTraversal.next();
            if (next instanceof MeanGlobalStep.MeanNumber){
                Double.isNaN(((MeanGlobalStep.MeanNumber) next).doubleValue());
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
}
