package org.unipop.elastic2.controller.schema.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by Roman on 4/28/2015.
 */
public class AggregationBuilder implements Cloneable {
    public enum Op {
        root,
        param,
        filters,
        filter,
        terms,
        count,
        min,
        max,
        stats,
        cardinality,
        having,

        innerAggregationBuilder
    }

    private enum FindMode {
        self,
        childrenOnly,
        selfWithChildren,
        full
    }

    //region Constructor
    public AggregationBuilder() {
        this.root = new RootComposite();
        this.current = this.root;
    }
    //endregion

    //region Public Methods
    public AggregationBuilder filters(String name) {
        if (this.current.op != Op.terms && this.current.op != Op.filters && this.current.op != Op.root) {
            throw new UnsupportedOperationException("'filters' may only appear in the 'root', 'terms' or 'filters' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite filters = new FiltersComposite(name, this.current);
            this.current.getChildren().add(filters);
            this.current = filters;
        }

        return this;
    }

    public AggregationBuilder filter(String name, QueryBuilder queryBuilder) {
        if (this.current.op != Op.terms && this.current.op != Op.filters && this.current.op != Op.root) {
            throw new UnsupportedOperationException("'filter' may only appear in the 'root', 'terms' or 'filters' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite filter = new FilterComposite(name, this.current, queryBuilder);
            this.current.getChildren().add(filter);
            this.current = filter;
        }

        return this;
    }

    public AggregationBuilder terms(String name) {
        if (this.current.op != Op.terms && this.current.op != Op.filters && this.current.op != Op.root) {
            throw new UnsupportedOperationException("'terms' may only appear in the 'root', 'terms' or 'filters' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite terms = new TermsComposite(name, this.current);
            this.current.getChildren().add(terms);
            this.current = terms;
        }

        return this;
    }

    public AggregationBuilder count(String name) {
        if (this.current.op == Op.root) {
            throw new UnsupportedOperationException("'count' may not appear as root aggregation");
        }

        if (this.current.op != Op.terms) {
            throw new UnsupportedOperationException("'count' may only appear in the 'terms' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite count = new CountComposite(name, this.current);
            this.current.getChildren().add(count);
            this.current = count;
        }

        return this;
    }

    public AggregationBuilder min(String name) {
        if (this.current.op == Op.root) {
            throw new UnsupportedOperationException("'min' may not appear as root aggregation");
        }

        if (this.current.op != Op.terms) {
            throw new UnsupportedOperationException("'min' may only appear in the 'terms' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite min = new MinComposite(name, this.current);
            this.current.getChildren().add(min);
            this.current = min;
        }

        return this;
    }

    public AggregationBuilder max(String name) {
        if (this.current.op == Op.root) {
            throw new UnsupportedOperationException("'max' may not appear as root aggregation");
        }

        if (this.current.op != Op.terms) {
            throw new UnsupportedOperationException("'max' may only appear in the 'terms' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite max = new MaxComposite(name, this.current);
            this.current.getChildren().add(max);
            this.current = max;
        }

        return this;
    }

    public AggregationBuilder stats(String name) {
        if (this.current.op == Op.root) {
            throw new UnsupportedOperationException("'stats' may not appear as root aggregation");
        }

        if (this.current.op != Op.terms) {
            throw new UnsupportedOperationException("'stats' may only appear in the 'terms' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite stats = new StatsComposite(name, this.current);
            this.current.getChildren().add(stats);
            this.current = stats;
        }

        return this;
    }

    public AggregationBuilder cardinality(String name) {
        if (this.current.op == Op.root) {
            throw new UnsupportedOperationException("'cardinality' may not appear as root aggregation");
        }

        if (this.current.op != Op.terms) {
            throw new UnsupportedOperationException("'cardinality' may only appear in the 'terms' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite cardinality = new CardinalityComposite(name, this.current);
            this.current.getChildren().add(cardinality);
            this.current = cardinality;
        }

        return this;
    }

    public <V> AggregationBuilder param(String name, V value) {
        if (this.current == this.root) {
            throw new UnsupportedOperationException("parameters may not be added to the root aggregation");
        }

        if (seekLocalParam(this.current, name) != null) {
            seekLocalParam(this.current, name).setValue(value);
        } else {
            Composite param = new ParamComposite(name, value, this.current);
            this.current.getChildren().add(param);
        }

        return this;
    }

    public AggregationBuilder field(String value) {
        return this.param("field", value);
    }

    public AggregationBuilder script(String value) {
        return this.param("script", value);
    }

    public AggregationBuilder size(int value) {
        return this.param("size", value);
    }

    public AggregationBuilder shardSize(int value) {
        return this.param("shard_size", value);
    }

    public AggregationBuilder executionHint(String value) {
        return this.param("execution_hint", value);
    }

    public AggregationBuilder precisionThreshold(long value) {
        return this.param("precision_threshold", value);
    }

    public AggregationBuilder collectMode(Aggregator.SubAggCollectionMode value) {
        return this.param("collect_mode", value);
    }

    public AggregationBuilder having(String name, BiPredicate predicate, Object value)       {
        if (this.current == this.root) {
            throw new UnsupportedOperationException("'having' may not be added to the root aggregation");
        }


        Composite having = new HavingComposite(new HasContainer(name, new P(predicate, value)), this.current);
        this.current.getChildren().add(having);

        return this;
    }

    public AggregationBuilder innerAggregationBuilder(String name, AggregationBuilder innerAggregationBuilder) {
        if (this.current.op != Op.terms && this.current.op != Op.root) {
            throw new UnsupportedOperationException("'innerAggregationBuilder' may only appear in the 'root' or 'terms' context");
        }

        if (this.seekLocalName(this.current, name) != null) {
            this.current = this.seekLocalName(this.current, name);
        } else {
            Composite innerAggregationBuilderComposite = new InnerAggregationBuilderComposite(name, innerAggregationBuilder, this.current);
            this.current.getChildren().add(innerAggregationBuilderComposite);
        }

        return this;
    }

    public Collection<Composite> find(String name) {
        return find(composite -> {
            if (composite == null) {
                return false;
            }

            return composite.getName() != null && composite.getName().equals(name);
        });
    }

    public AggregationBuilder seek(String name) {
        return seek(composite -> {
            if (composite == null) {
                return false;
            }

            return composite.getName() != null && composite.getName().equals(name);
        });
    }

    public AggregationBuilder seek(Composite compositeSeek) {
        return seek(composite -> {
            if (composite == null) {
                return false;
            }

            return composite.equals(compositeSeek);
        });
    }

    public AggregationBuilder seek(Predicate<Composite> predicate) {
        Collection<Composite> seek = this.root.find(predicate, FindMode.full);
        if (seek != null && seek.size() > 0) {
            this.current = seek.stream().findFirst().get();
        }

        return this;
    }

    public Collection<Composite> find(Predicate<Composite> predicate) {
        Collection<Composite> composites = this.root.find(predicate, FindMode.full);
        return composites;
    }

    public AggregationBuilder seekRoot() {
        this.current = this.root;
        return this;
    }

    public AggregationBuilder clear() {
        this.current.clear();
        return this;
    }

    public Iterable<org.elasticsearch.search.aggregations.AggregationBuilder> getAggregations() {
        return (Iterable< org.elasticsearch.search.aggregations.AggregationBuilder>)root.build();
    }

    // The clone will return a deep clone of the aggregation builder (except leaf values: e.g the Object value in terms composite).
    // The clone will set the current field to point to the root due to the difficulty in finding the cloned current composite in the clone AggregationBuilder.
    @Override
    public AggregationBuilder clone() {
        try {
            AggregationBuilder clone = (AggregationBuilder) super.clone();
            clone.root = root.clone();
            clone.current = clone.root;
            return clone;
        } catch(CloneNotSupportedException ex){
            return null;
        }
    }
    //endregion

    //region Properties
    public Composite getRoot() {
        return this.root;
    }

    public Composite getCurrent() {
        return this.current;
    }
    //endregion

    //region Private Methods
    private Composite seekLocalName(Composite composite, String name) {
        Optional<Composite> find = composite.find(childComposite -> {
            if (childComposite == null) {
                return false;
            }

            return childComposite.getName() != null && childComposite.getName().equals(name);
        }, FindMode.childrenOnly).stream().findFirst();

        if (find.isPresent()) {
            return find.get();
        } else {
            return null;
        }
    }

    private Composite seekLocalClass(Composite composite, Class<? extends Composite> compositeClass){
        Optional<Composite> find = composite.find(childComposite -> {
            if (childComposite == null) {
                return false;
            }

            return childComposite.getClass().equals(compositeClass);
        }, FindMode.childrenOnly).stream().findFirst();

        if (find.isPresent()) {
            return find.get();
        } else {
            return null;
        }
    }

    private ParamComposite seekLocalParam(Composite composite, String name) {
        Optional<ParamComposite> find = composite.find(childComposite -> {
            if (childComposite == null) {
                return false;
            }

            return childComposite.getName() != null && childComposite.getName().equals(name) &&
                    ParamComposite.class.isAssignableFrom(childComposite.getClass());
        }, FindMode.childrenOnly).stream().map(c -> (ParamComposite)c).findFirst();

        if (find.isPresent()) {
            return find.get();
        } else {
            return null;
        }
    }
    //endregion

    //region Fields
    private Composite root;
    private Composite current;
    //endregion

    //region Composite
    public abstract class Composite implements Cloneable {

        //region Constructor
        public Composite(String name, Op op, Composite parent) {
            this.name = name;
            this.op = op;
            this.parent = parent;

            this.children = new ArrayList<>();
        }
        //endregion

        //region Abstract Methods
        protected abstract Object build();
        //endregion

        //region Protected Methods
        protected Collection<Composite> find(Predicate<Composite> predicate, FindMode findMode) {
            List<Composite> foundComposites = new ArrayList<>();

            if (findMode != FindMode.childrenOnly) {
                if (predicate.test(this)) {
                    foundComposites.add(this);
                }
            }

            if ((findMode == FindMode.childrenOnly || findMode == FindMode.selfWithChildren || findMode == FindMode.full)
                    && this.getChildren() != null) {
                for (Composite child : this.getChildren()) {
                    if (findMode == FindMode.full) {
                        foundComposites.addAll(child.find(predicate, findMode));
                    } else {
                        if (predicate.test(child)) {
                            foundComposites.add(child);
                        }
                    }
                }
            }

            return foundComposites;
        }

        protected void clear() {
            this.getChildren().clear();
        }

        @Override
        protected Composite clone() throws CloneNotSupportedException {
            Composite clone = (Composite) super.clone();
            clone.children = new ArrayList<>();
            for (Composite child : this.getChildren()) {
                Composite childClone = child.clone();

                clone.children.add(childClone);
                childClone.parent = clone;
            }

            return clone;
        }
        //endregion

        //region Properties
        public String getName() {
            return name;
        }

        public Op getOp() {
            return op;
        }

        public List<Composite> getChildren() {
            return children;
        }

        public Composite getParent() {
            return parent;
        }
        //endregion

        //region Fields
        private String name;
        private Op op;
        private Composite parent;

        private List<Composite> children;
        //endregion
    }

    public class RootComposite extends Composite {
        //region Constructor
        public RootComposite() {
            super(null, Op.root, null);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return this.getChildren().stream()
                    .map(child -> (org.elasticsearch.search.aggregations.AggregationBuilder) child.build()).collect(Collectors.toList());
        }
        //endregion
    }

    public class FiltersComposite extends Composite {
        //region Constructor
        public FiltersComposite(String name, Composite parent) {
            super(name, Op.filters, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            Map<String, org.elasticsearch.index.query.QueryBuilder> queryMap = new HashMap<>();
            for (FilterComposite filter : this.getChildren().stream()
                    .filter(child -> FilterComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (FilterComposite) child).collect(Collectors.toList())) {

                org.elasticsearch.index.query.QueryBuilder queryBuilder = (org.elasticsearch.index.query.QueryBuilder)filter.queryBuilder.seekRoot().query().filtered().filter().getCurrent().build();
                queryMap.put(filter.getName(), queryBuilder);
            }

            FiltersAggregationBuilder filtersAggsBuilder = AggregationBuilders.filters(this.getName());
            for(Map.Entry<String, org.elasticsearch.index.query.QueryBuilder> entry : queryMap.entrySet()) {
                filtersAggsBuilder.filter(entry.getKey(), entry.getValue());
            }

            for (Composite childComposite : this.getChildren().stream()
                    .filter(child -> !FilterComposite.class.isAssignableFrom(child.getClass()) &&
                            !ParamComposite.class.isAssignableFrom(child.getClass()) &&
                            !HavingComposite.class.isAssignableFrom(child.getClass())).collect(Collectors.toList())) {

                Object childAggregation = childComposite.build();

                if (AbstractAggregationBuilder.class.isAssignableFrom(childAggregation.getClass())) {
                    AbstractAggregationBuilder childAggregationBuilder = (AbstractAggregationBuilder) childComposite.build();
                    if (childAggregationBuilder != null) {
                        filtersAggsBuilder.subAggregation((AbstractAggregationBuilder) childComposite.build());
                    }
                } else if (Iterable.class.isAssignableFrom(childAggregation.getClass())) {
                    Iterable<AbstractAggregationBuilder> childAggregationBuilders = (Iterable<AbstractAggregationBuilder>)childAggregation;
                    for(AbstractAggregationBuilder childAggregationBuilder : childAggregationBuilders) {
                        if (childAggregationBuilder != null) {
                            filtersAggsBuilder.subAggregation((AbstractAggregationBuilder) childComposite.build());
                        }
                    }
                }
            }

            return filtersAggsBuilder;
        }
        //endregion
    }

    public class FilterComposite extends Composite {
        //region Constructor
        public FilterComposite(String name, Composite parent, QueryBuilder queryBuilder) {
            super(name, Op.filter, parent);
            this.queryBuilder = queryBuilder;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return AggregationBuilders.filter(this.getName()).
                    filter((org.elasticsearch.index.query.QueryBuilder)this.queryBuilder.seekRoot().query().filtered().filter().getCurrent().build());
        }

        @Override
        protected Composite clone() throws CloneNotSupportedException {
            FilterComposite clone = (FilterComposite) super.clone();
            clone.queryBuilder = this.queryBuilder.clone();

            return clone;
        }
        //endregion

        //region Fields
        private QueryBuilder queryBuilder;
        //endregion
    }

    public class TermsComposite extends Composite {
        //region Constructor
        public TermsComposite(String name, Composite parent) {
            super(name, Op.terms, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            TermsBuilder terms = AggregationBuilders.terms(this.getName());

            for (ParamComposite param : this.getChildren().stream()
                    .filter(child -> ParamComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (ParamComposite) child).collect(Collectors.toList())) {
                switch (param.getName().toLowerCase()) {
                    case "field":
                        terms.field((String)param.getValue());
                        break;

                    case "size":
                        terms.size((int)param.getValue());
                        break;

                    case "shard_size":
                        terms.shardSize((int)param.getValue());
                        break;

                    case "execution_hint":
                        terms.executionHint((String)param.getValue());
                        break;

                    case "collect_mode":
                        terms.collectMode((Aggregator.SubAggCollectionMode)param.getValue());
                        break;
                }
            }

            for (Composite childComposite : this.getChildren().stream()
                    .filter(child -> !ParamComposite.class.isAssignableFrom(child.getClass()) &&
                            !HavingComposite.class.isAssignableFrom(child.getClass())).collect(Collectors.toList())) {

                Object childAggregation = childComposite.build();

                if (AbstractAggregationBuilder.class.isAssignableFrom(childAggregation.getClass())) {
                    AbstractAggregationBuilder childAggregationBuilder = (AbstractAggregationBuilder) childComposite.build();
                    if (childAggregationBuilder != null) {
                        terms.subAggregation((AbstractAggregationBuilder) childComposite.build());
                    }
                } else if (Iterable.class.isAssignableFrom(childAggregation.getClass())) {
                    Iterable<AbstractAggregationBuilder> childAggregationBuilders = (Iterable<AbstractAggregationBuilder>)childAggregation;
                    for(AbstractAggregationBuilder childAggregationBuilder : childAggregationBuilders) {
                        if (childAggregationBuilder != null) {
                            terms.subAggregation((AbstractAggregationBuilder) childComposite.build());
                        }
                    }
                }
            }

            return terms;
        }
        //endregion
    }

    public class CountComposite extends Composite {
        //region Constructor
        public CountComposite(String name, Composite parent) {
            super(name, Op.count, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            String countField = null;
            for (ParamComposite param : this.getChildren().stream()
                    .filter(child -> ParamComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (ParamComposite) child).collect(Collectors.toList())) {
                switch (param.getName().toLowerCase()) {
                    case "field":
                        countField = (String)param.getValue();
                        break;
                }
            }

            if (countField != null) {
                ValueCountBuilder count = AggregationBuilders.count(this.getName());
                count.field(countField);
                return count;
            }

            return null;
        }
        //endregion
    }

    public class MinComposite extends Composite {
        //region Constructor
        public MinComposite(String name, Composite parent) {
            super(name, Op.min, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            MinBuilder min = AggregationBuilders.min(this.getName());

            for (ParamComposite param : this.getChildren().stream()
                    .filter(child -> ParamComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (ParamComposite) child).collect(Collectors.toList())) {
                switch (param.getName().toLowerCase()) {
                    case "field":
                        min.field((String)param.getValue());
                }
            }

            return min;
        }
        //endregion
    }

    public class MaxComposite extends Composite {
        //region Constructor
        public MaxComposite(String name, Composite parent) {
            super(name, Op.max, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            MaxBuilder max = AggregationBuilders.max(this.getName());

            for (ParamComposite param : this.getChildren().stream()
                    .filter(child -> ParamComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (ParamComposite) child).collect(Collectors.toList())) {
                switch (param.getName().toLowerCase()) {
                    case "field":
                        max.field((String)param.getValue());
                }
            }

            return max;
        }
        //endregion
    }

    public class StatsComposite extends Composite {
        //region Constructor
        public StatsComposite(String name, Composite parent) {
            super(name, Op.stats, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            StatsBuilder stats = AggregationBuilders.stats(this.getName());

            for (ParamComposite param : this.getChildren().stream()
                    .filter(child -> ParamComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (ParamComposite) child).collect(Collectors.toList())) {
                switch (param.getName().toLowerCase()) {
                    case "field":
                        stats.field((String)param.getValue());
                }
            }

            return stats;
        }
        //endregion
    }

    public class CardinalityComposite extends Composite {
        //region Constructor
        public CardinalityComposite(String name, Composite parent) {
            super(name, Op.cardinality, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            CardinalityBuilder cardinality = AggregationBuilders.cardinality(this.getName());

            for (ParamComposite param : this.getChildren().stream()
                    .filter(child -> ParamComposite.class.isAssignableFrom(child.getClass()))
                    .map(child -> (ParamComposite) child).collect(Collectors.toList())) {
                switch (param.getName().toLowerCase()) {
                    case "field":
                        cardinality.field((String) param.getValue());
                        break;

                    case "script":
                        cardinality.script((Script) param.getValue());
                        break;

                    case "precision_threshold":
                        cardinality.precisionThreshold((long) param.getValue());
                        break;
                }
            }

            return cardinality;
        }
        //endregion
    }

    public class ParamComposite<V> extends Composite {
        //region Constructor
        public ParamComposite(String name, V value, Composite parent) {
            super(name, Op.param, parent);
            this.value = value;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return null;
        }
        //endregion

        //region Properties
        public V getValue() {
            return this.value;
        }

        public void setValue(V value) {
            this.value = value;
        }
        //endregion

        //region Fields
        private V value;
        //endregion
    }

    public class HavingComposite extends Composite {
        //region Constructor
        public HavingComposite(HasContainer hasContainer, Composite parent) {
            super(hasContainer.getKey(), Op.having, parent);
            this.hasContainer = hasContainer;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return null;
        }
        //endregion

        //region Properties
        public HasContainer getHasContainer() {
            return this.hasContainer;
        }
        //endregion

        //region Fields
        private HasContainer hasContainer;
        //endregion
    }

    public class InnerAggregationBuilderComposite extends Composite {
        //region Constructor
        public InnerAggregationBuilderComposite(String name, AggregationBuilder innerAggregationBuilder, Composite parent) {
            super(name, Op.innerAggregationBuilder, parent);
            this.innerAggregationBuilder = innerAggregationBuilder;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return innerAggregationBuilder.getAggregations();
        }

        @Override
        protected Collection<Composite> find(Predicate<Composite> predicate, FindMode findMode) {
            Collection<Composite> composites = super.find(predicate, findMode);

            if (findMode == FindMode.full) {
                composites.addAll(this.innerAggregationBuilder.getRoot().find(predicate, findMode));
            }

            return composites;
        }

        @Override
        protected Composite clone() throws CloneNotSupportedException {
            InnerAggregationBuilderComposite clone = (InnerAggregationBuilderComposite) super.clone();
            clone.innerAggregationBuilder = this.innerAggregationBuilder.clone();

            return clone;
        }
        //endregion

        //region Fields
        private AggregationBuilder innerAggregationBuilder;
        //endregion
    }
}
