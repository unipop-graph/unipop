package org.unipop.process.reduce;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.process.predicate.ReceivesPredicatesHolder;
import org.unipop.process.properties.PropertyFetcher;
import org.unipop.query.StepDescriptor;
import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.query.aggregation.reduce.ReduceVertexQuery;
import org.unipop.query.controller.ControllerManager;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Gur Ronen
 * @since 6/27/2016
 * <p>
 * This step is the replacement for reducing functions in gremlin to Unipop implementations
 * {@link org.unipop.query.aggregation.reduce.ReduceQuery.Op} for a list of reduce methods.
 */
public class UniGraphReduceStep<S extends Element> extends ReducingBarrierStep<S, Number> implements ReceivesPredicatesHolder<S, Number>, PropertyFetcher {

    private PredicatesHolder predicatesHolder;
    private ControllerManager controllerManager;
    private StepDescriptor stepDescriptor;
    private ReduceQuery.Op op;
    private Direction direction;
    private Class elementClass;
    private Set<String> propertyKeys;
    private int limit;

    private List<S> bulk;

    //region Constructor
    public UniGraphReduceStep(
            Traversal.Admin traversal,
            Class elementClass,
            ControllerManager controllerManager,
            ReduceQuery.Op op) {

        super(traversal);
        this.predicatesHolder = PredicatesHolderFactory.empty();
        this.controllerManager = controllerManager;
        this.stepDescriptor = new StepDescriptor(this);
        this.op = op;
        this.elementClass = elementClass;
        this.propertyKeys = Sets.newHashSet();
        this.bulk = Lists.newArrayList();

        Supplier<Number> numberSupplier = op.getSeedSupplier();
        this.setSeedSupplier(numberSupplier);
        /**
         * needed for multiple controller result.
         * if we have 3 controllers all doing count, we need to do ourselves the sum between them
         */
        this.setReducingBiOperator(op.getOperator());
    }

    //endregion

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.BULK);
    }

    @Override
    public void addPropertyKey(String key) {
        if (this.propertyKeys != null) {
            this.propertyKeys.add(key);
        }
    }

    @Override
    public void fetchAllKeys() {
        this.propertyKeys = null;
    }

    @Override
    public void addPredicate(PredicatesHolder predicatesHolder) {
        this.predicatesHolder = PredicatesHolderFactory.and(this.predicatesHolder, predicatesHolder);
    }

    @Override
    public PredicatesHolder getPredicates() {
        return this.predicatesHolder;
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    // TODO: Implement Vertex Query in the future to improve
    // TODO: g.V().out().out().count() - g.V().out.count()
    @Override
    public Number projectTraverser(Traverser.Admin<S> traverser) {
        if (!this.starts.hasNext()) {
            return executeReduceQuery();
        }
        this.bulk.add(traverser.get());
        return this.getSeedSupplier().get();
    }

    private Number executeReduceQuery() {
        ReduceQuery query = new ReduceQuery(this.predicatesHolder, this.stepDescriptor, this.op, this.propertyKeys);

        return this.controllerManager.getControllers(ReduceQuery.ReduceController.class).stream()
                .map(rc -> rc.reduce(query)).reduce(this.getBiOperator()).orElseGet(this.getSeedSupplier());
    }


    private Number executeReduceVertexQuery(List<Vertex> vertices, Direction dir) {
        ReduceVertexQuery query = new ReduceVertexQuery(this.predicatesHolder, this.stepDescriptor, vertices, dir, this.op, this.propertyKeys);

        return this.controllerManager.getControllers(ReduceVertexQuery.ReduceVertexController.class).stream()
                .map(rc -> rc.reduce(query)).reduce(this.getBiOperator()).orElseGet(this.getSeedSupplier());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }
}
