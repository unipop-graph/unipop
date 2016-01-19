package org.unipop.elastic2.controller.schema.helpers;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Created by Roman on 3/22/2015.
 */
public class QueryBuilder implements Cloneable{
    public enum Op {
        query,
        matchAll,
        filtered,
        filter,
        bool,
        must,
        mustNot,
        should,
        term,
        terms,
        range,
        ids,
        type,
        exists
    }

    private enum SeekMode {
        self,
        childrenOnly,
        selfWithChildren,
        full
    }

    public static class Keywords {
       // public static Set<String> getSet() {
        //    return set;
       // }
        //private static Set<String> set = ImmutableSet.of("$(ids)");
    }

    //region Constructor
    public QueryBuilder() {
        int x = 5;
    }
    //endregion

    //region Public Methods
    public QueryBuilder query() {
        return query(null);
    }

    public QueryBuilder query(String name) {
        if (this.root == null) {
            this.root = new QueryComposite(name, null);
            this.current = root;
        } else {
            if (this.current == root) {
                return this;
            }

            if (this.current.getOp() != Op.filtered) {
                throw new UnsupportedOperationException("'query' can only appear as root or immediately after 'filtered'");
            }

            if (seekLocalClass(current, QueryComposite.class) != null) {
                this.current = seekLocalClass(current, QueryComposite.class);
            } else {
                Composite queryComposite = new QueryComposite(name, current);
                this.current.children.add(queryComposite);
                this.current = queryComposite;
            }
        }

        return this;
    }

    public QueryBuilder matchAll() {
        if (this.root == null) {
            throw new UnsupportedOperationException("'matchAll' may not appear as first statement");
        }

        if (this.current.op != Op.query) {
            throw new UnsupportedOperationException("'matchAll' may only appear in the 'query' context");
        }

        Composite matchAllComposite = new MatchAllComposite(null, current);
        this.current.clear();
        this.current.children.add(matchAllComposite);

        return this;
    }

    public QueryBuilder filtered() {
        return filtered(null);
    }

    public QueryBuilder filtered(String name) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'filtered' may not appear as first statement");
        }

        if (this.current != root) {
            throw new UnsupportedOperationException("'filtered' may only appear immediately after root 'query'");
        }

        if (seekLocalClass(current, FilteredComposite.class) != null) {
            this.current = seekLocalClass(current, FilteredComposite.class);
        } else {
            Composite filteredComposite = new FilteredComposite(name, current);
            this.current.clear();
            this.current.children.add(filteredComposite);
            this.current = filteredComposite;
        }

        return this;
    }

    public QueryBuilder filter() {
        return filter(null);
    }

    public QueryBuilder filter(String name) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'filter' may not appear as first statement");
        }

        if (this.current.op != Op.filtered) {
            throw new UnsupportedOperationException("'filter' may only appear in the 'filtered' context");
        }

        if (seekLocalClass(current, FilterComposite.class) != null) {
            this.current = seekLocalClass(current, FilterComposite.class);
        } else {
            Composite filterComposite = new FilterComposite(name, current);
            this.current.children.add(filterComposite);
            this.current = filterComposite;
        }

        return this;
    }

    public QueryBuilder bool() {
        return bool(null);
    }

    public QueryBuilder bool(String name) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'bool' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'bool' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        if (StringUtils.isNotBlank(name) && seekLocalName(current, name) != null) {
            this.current = seekLocalName(current, name);
            return this;
        }

        if (this.current.op == Op.filter && seekLocalClass(current, BoolComposite.class) != null) {
            this.current = seekLocalClass(current, BoolComposite.class);
            return this;
        }

        Composite boolComposite = new BoolComposite(name, current);
        this.current.children.add(boolComposite);
        this.current = boolComposite;

        return this;
    }

    public QueryBuilder must() {
        return must(null);
    }

    public QueryBuilder must(String name) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'must' may not appear as first statement");
        }

        if (this.current.op != Op.bool) {
            throw new UnsupportedOperationException("'must' may only appear in the 'bool' context");
        }

        if (seekLocalClass(current, MustComposite.class) != null) {
            this.current = seekLocalClass(current, MustComposite.class);
        } else {
            Composite mustComposite = new MustComposite(name, current);
            this.current.children.add(mustComposite);
            this.current = mustComposite;
        }

        return this;
    }

    public QueryBuilder mustNot() {
        return mustNot(null);
    }

    public QueryBuilder mustNot(String name) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'mustNot' may not appear as first statement");
        }

        if (this.current.op != Op.bool) {
            throw new UnsupportedOperationException("'mustNot' may only appear in the 'bool' context");
        }

        if (seekLocalClass(current, MustNotComposite.class) != null) {
            this.current = seekLocalClass(current, MustNotComposite.class);
        } else {
            Composite mustNotComposite = new MustNotComposite(name, current);
            this.current.children.add(mustNotComposite);
            this.current = mustNotComposite;
        }

        return this;
    }

    public QueryBuilder should() {
        return should(null);
    }

    public QueryBuilder should(String name) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'should' may not appear as first statement");
        }

        if (this.current.op != Op.bool) {
            throw new UnsupportedOperationException("'should' may only appear in the 'bool' context");
        }

        if (seekLocalClass(current, ShouldComposite.class) != null) {
            this.current = seekLocalClass(current, ShouldComposite.class);
        } else {
            Composite shouldComposite = new ShouldComposite(name, current);
            this.current.children.add(shouldComposite);
            this.current = shouldComposite;
        }

        return this;
    }

    public QueryBuilder term(String name, Object value) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'term' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'term' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        Composite termComposite = new TermComposite(name, value, current);
        this.current.children.add(termComposite);

        return this;
    }

    public QueryBuilder terms(String name, Object value) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'terms' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'terms' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        if (!(Iterable.class.isAssignableFrom(value.getClass()))) {
            throw new IllegalArgumentException("illegal value argument for 'terms': " + value.getClass().getSimpleName());
        }

        Composite termComposite = new TermsComposite(name, value, current);
        this.current.children.add(termComposite);

        return this;
    }

    public QueryBuilder range(String name, Compare compare, Object value) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'range' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'range' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        Composite rangeComposite = new RangeComposite(new HasContainer(name, new P(compare, value)), current);
        this.current.children.add(rangeComposite);

        return this;
    }

    public QueryBuilder range(String name, Object from, Object to) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'range' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'range' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        Composite rangeComposite = new RangeComposite(name, from, to, current);
        this.current.children.add(rangeComposite);

        return this;
    }

    public QueryBuilder ids(Object value) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'ids' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'ids' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        if (!(value instanceof Iterable)) {
            throw new IllegalArgumentException("illegal value argument for 'ids'");
        }

        Composite idsComposite = new IdsComposite(value, current);
        this.current.children.add(idsComposite);

        return this;
    }

    public QueryBuilder ids(Iterable<String> ids, String... types) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'ids' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'ids' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        Composite idsComposite = new IdsComposite(ids, types, current);
        this.current.children.add(idsComposite);

        return this;
    }

    public QueryBuilder type(String value) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'type' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'type' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        Composite typeComposite = new TypeComposite(value, current);
        this.current.children.add(typeComposite);

        return this;
    }

    public QueryBuilder exists(String value) {
        if (this.root == null) {
            throw new UnsupportedOperationException("'exists' may not appear as first statement");
        }

        if (this.current.op != Op.filter && current.op != Op.must && current.op != Op.mustNot && current.op != Op.should) {
            throw new UnsupportedOperationException("'exists' may only appear in the 'filter', 'must', 'mustNot' or 'should' context");
        }

        Composite existsComposite = new ExistsComposite(value, current);
        this.current.children.add(existsComposite);

        return this;
    }

    public QueryBuilder seek(String name) {
        return seek(composite -> {
            if (composite == null) {
                return false;
            }

            return composite.getName() != null && composite.getName().equals(name);
        });
    }

    public QueryBuilder seek(Composite compositeSeek) {
        return seek(composite -> {
            if (composite == null) {
                return false;
            }

            return composite.equals(compositeSeek);
        });
    }

    public QueryBuilder seek(Predicate<Composite> predicate) {
        Composite seek = this.root.seek(predicate, SeekMode.full);
        if (seek != null) {
            this.current = seek;
        }

        return this;
    }

    public QueryBuilder seekRoot() {
        this.current = this.root;
        return this;
    }

    public QueryBuilder expand(Map<String, Object> expandValues) {
        this.root.expand(expandValues);
        return this;
    }

    public QueryBuilder clear() {
        if (this.current != null) {
            this.current.clear();
            if (this.current == this.root) {
                this.current = null;
                this.root = null;
            }
        }

        return this;
    }

    public org.elasticsearch.index.query.QueryBuilder getQuery() {
        if (root == null) {
            return null;
        }

        return (org.elasticsearch.index.query.QueryBuilder)root.build();
    }

    // The clone will return a deep clone of the query builder (except leaf values: e.g the Object value in terms composite).
    // The clone will set the current field to point to the root due to the difficulty in finding the cloned current composite in the clone QueryBuilder.
    @Override
    public QueryBuilder clone() {
        try {
            QueryBuilder clone = (QueryBuilder) super.clone();
            if (root != null) {
                clone.root = root.clone();
            }
            clone.current = clone.root;
            return clone;
        } catch(CloneNotSupportedException ex){
            return null;
        }
    }

    public boolean isValid() {
        List<Op> parentOps = Arrays.asList(Op.query, Op.filtered, Op.filter, Op.bool, Op.must, Op.mustNot, Op.should);
        Composite invalidComposite = this.root.seek(composite -> {
            return parentOps.contains(composite.getOp()) &&
                    (composite.getChildren() == null || composite.getChildren().size() == 0);
        }, SeekMode.full);

        return invalidComposite == null;
    }

    public boolean hasFilters() {
        List<Op> filterOps = Arrays.asList(Op.exists, Op.ids, Op.range, Op.term, Op.terms, Op.type);
        Composite filter = this.root.seek(composite -> {
            return filterOps.contains(composite.getOp());
        }, SeekMode.full);

        return filter != null;
    }

    public<T> T visit(String labelNodeToSeek, BiFunction<Composite, T, T> accumulator, T seed) {
        QueryBuilder queryBuilder = this.seek(labelNodeToSeek);
        return visit(queryBuilder.current, accumulator, seed);
    }

    public<T> T visit(Composite composite, BiFunction<Composite, T, T> accumulator, T seed) {
        if (composite == null) {
            return null;
        }

        T result = accumulator.apply(composite, seed);

        for (Composite child : composite.getChildren()) {
            result = visit(child, accumulator, result);
        }

        return result;
    }

    //endregion

    //region Properties
    public Composite getCurrent() {
        return this.current;
    }
    //endregion

    //region Private Methods
    private Composite seekLocalName(Composite composite, String name) {
        return composite.seek(childComposite -> {
            if (childComposite == null) {
                return false;
            }

            return childComposite.getName() != null && childComposite.getName().equals(name);
        }, SeekMode.childrenOnly);
    }

    private Composite seekLocalClass(Composite composite, Class<? extends Composite> compositeClass){
        return composite.seek(childComposite -> {
            if (childComposite == null) {
                return false;
            }

            return childComposite.getClass().equals(compositeClass);
        }, SeekMode.childrenOnly);
    }
    //endregion

    //region Fields
    private Composite root;
    private Composite current;
    //endregion

    //region Composite
    public abstract class Composite implements Cloneable{

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
        protected Composite seek(Predicate<Composite> predicate, SeekMode seekMode) {
            if (seekMode != SeekMode.childrenOnly) {
                if (predicate.test(this)) {
                    return this;
                }
            }

            if ((seekMode == SeekMode.childrenOnly || seekMode == SeekMode.selfWithChildren || seekMode == SeekMode.full)
                    && this.getChildren() != null) {
                for (Composite child : this.getChildren()) {
                    if (seekMode == SeekMode.full) {
                        Composite childSeek = child.seek(predicate, seekMode);
                        if (childSeek != null) {
                            return childSeek;
                        }
                    } else {
                        if (predicate.test(child)) {
                            return child;
                        }
                    }
                }
            }

            return null;
        }

        protected void expand(Map<String, Object> expandValues) {
            if (this.getChildren() != null) {
                for(Composite child : this.getChildren()) {
                    child.expand(expandValues);
                }
            }
        }

        protected void clear() {
            this.getChildren().clear();
        }

        @Override
        protected Composite clone() throws CloneNotSupportedException{
            Composite clone = (Composite)super.clone();
            clone.children = new ArrayList<>();
            for(Composite child : this.getChildren()) {
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

        protected List<Composite> getChildren() {
            return children;
        }

        protected Composite getParent() {
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

    public class QueryComposite extends Composite {
        //region Constructor
        protected QueryComposite(String name, Composite parent) {
            super(name, Op.query, parent);
        }
        //endregion

        //region Fields
        @Override
        protected Object build() {
            return getChildren().get(0).build();
        }
        //endregion
    }

    public class FilteredComposite extends Composite {
        //region Constructor
        protected FilteredComposite(String name, Composite parent) {
            super(name, Op.filtered, parent);
        }
        //endregion

        //region Composite
        @Override
        protected Object build() {
            org.elasticsearch.index.query.BoolQueryBuilder boolQueryBuilder = boolQuery();
            org.elasticsearch.index.query.QueryBuilder queryBuilder = matchAllQuery();
            org.elasticsearch.index.query.QueryBuilder filterBuilder = matchAllQuery();

            for(Composite child : getChildren()) {
                if (child.getOp() == Op.query) {
                    queryBuilder = (org.elasticsearch.index.query.QueryBuilder) child.build();
                } else if (child.getOp() == Op.filter) {
                    filterBuilder = (org.elasticsearch.index.query.QueryBuilder) child.build();
                }
            }

            return boolQueryBuilder.filter(queryBuilder).filter(filterBuilder);
        }
        //endregion
    }

    public class FilterComposite extends Composite {
        //region Constructor
        protected FilterComposite(String name, Composite parent) {
            super(name, Op.filter, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return getChildren().get(0).build();
        }
        //endregion
    }

    public class BoolComposite extends Composite {
        //region Constructor
        protected BoolComposite(String name, Composite parent) {
            super(name, Op.bool, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            Iterable<org.elasticsearch.index.query.QueryBuilder> mustFilters = new ArrayList<>();
            Iterable<org.elasticsearch.index.query.QueryBuilder> mustNotFilters = new ArrayList<>();
            Iterable<org.elasticsearch.index.query.QueryBuilder> shouldFilters = new ArrayList<>();

            for(Composite child : getChildren()) {
                switch (child.getOp()) {
                    case must:
                        mustFilters = (Iterable<org.elasticsearch.index.query.QueryBuilder>) child.build();
                        break;

                    case mustNot:
                        mustNotFilters = (Iterable<org.elasticsearch.index.query.QueryBuilder>) child.build();
                        break;

                    case should:
                        shouldFilters = (Iterable<org.elasticsearch.index.query.QueryBuilder>) child.build();
                        break;
                }
            }

            BoolQueryBuilder boolQueryBuilder = boolQuery();
            mustFilters.forEach(boolQueryBuilder::filter);
            mustNotFilters.forEach(boolQueryBuilder::mustNot);
            shouldFilters.forEach(boolQueryBuilder::should);
            return boolQueryBuilder;
        }
        //endregion
    }

    public abstract class FiltersComposite extends Composite {
        //region Constructor
        protected FiltersComposite(String name, Op op, Composite parent) {
            super(name, op, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            ArrayList<org.elasticsearch.index.query.QueryBuilder> filters = new ArrayList<>();
            for(Composite child : getChildren()) {
                filters.add((org.elasticsearch.index.query.QueryBuilder) child.build());
            }
            return filters;
        }
        //endregion
    }

    public class MustComposite extends FiltersComposite {

        protected MustComposite(String name, Composite parent) {
            super(name, Op.must, parent);
        }
    }

    public class MustNotComposite extends FiltersComposite {

        protected MustNotComposite(String name, Composite parent) {
            super(name, Op.mustNot, parent);
        }
    }

    public class ShouldComposite extends FiltersComposite {

        protected ShouldComposite(String name, Composite parent) {
            super(name, Op.should, parent);
        }
    }

    public class TermComposite extends Composite {
        //region Constructor
        protected TermComposite(String name, Object value, Composite parent) {
            super(name, Op.term, parent);
            this.value = value;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return termQuery(getName(), this.value);
        }
        //endregion

        //region Fields
        private Object value;
        //endregion
    }

    public class TermsComposite extends Composite {
        //region Constructor
        protected TermsComposite(String name, Object value, Composite parent) {
            super(name, Op.terms, parent);
            this.value = value;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            if (this.value instanceof Iterable) {
                return termsQuery(getName(), StreamSupport.stream(((Iterable) value).spliterator(), false).toArray());
            }

            return termsQuery(getName(), this.value);
        }

        @Override
        protected void expand(Map<String, Object> expandValues) {
            super.expand(expandValues);

            if (this.value instanceof String && this.value.toString().startsWith("$")) {
                Object newValue = expandValues.get(this.value);

                if (newValue == null || !(newValue instanceof Iterable)) {
                    throw new IllegalArgumentException("illegal expand value for 'terms'");
                }

                this.value = newValue;
            }
        }
        //endregion

        //region Fields
        private Object value;
        //endregion
    }

    public class RangeComposite extends Composite {

        //region Constructor
        protected RangeComposite(String name, Object from, Object to, Composite parent) {
            super(name, Op.range, parent);

            this.from = from;
            this.to = to;
        }

        protected RangeComposite(HasContainer has, Composite parent) {
            super(has.getKey(), Op.range, parent);

            this.has = has;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            if (this.from != null && this.to != null) {
                return rangeQuery(getName()).from(this.from).to(this.to);
            } else if (has != null ) {
                if (has.getBiPredicate() instanceof Compare) {
                    Compare compare = (Compare)has.getBiPredicate();

                    switch (compare) {
                        case gt: return rangeQuery(getName()).gt(has.getValue());
                        case gte: return rangeQuery(getName()).gte(has.getValue());
                        case lt: return rangeQuery(getName()).lt(has.getValue());
                        case lte: return rangeQuery(getName()).lte(has.getValue());
                        default: throw new UnsupportedOperationException("range filter can only be built using gt, gte, lt, lte compare predicates");
                    }
                }
            }

            throw new UnsupportedOperationException("range filter can only be built with full range using 'from' and 'to' or using the Compare predicate");
        }
        //endregion

        //region Fields
        private Object from;
        private Object to;

        private HasContainer has;
        //endregion
    }

    public class IdsComposite extends Composite {
        //region Constructor
        public IdsComposite(Object value, Composite parent) {
            super(null, Op.ids, parent);
            this.value = value;
            types = new String[0];
        }

        public IdsComposite(Iterable<String> ids, String[] types, Composite parent) {
            super(null, Op.ids, parent);
            this.value = ids;
            this.types = types;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            if (this.value instanceof Iterable) {
                ArrayList<String> ids = new ArrayList<>();
                for(Object obj : (Iterable)this.value) {
                    ids.add(obj.toString());
                }

                return idsQuery(this.types).ids(ids.stream().toArray(String[]::new));
            }

            return idsQuery(this.types).ids(this.value.toString());
        }

        @Override
        protected void expand(Map<String, Object> expandValues) {
            super.expand(expandValues);

            if (this.value instanceof String && this.value.toString().startsWith("$")) {
                Object newValue = expandValues.get(this.value);

                if (newValue == null || !(newValue instanceof Iterable)) {
                    throw new IllegalArgumentException("illegal expand value for 'terms'");
                }

                this.value = newValue;
            }
        }
        //endregion

        //region Fields
        private Object value;
        private String[] types;
        //endregion
    }

    public class TypeComposite extends Composite {
        //region Constructor
        public TypeComposite(String value, Composite parent) {
            super(null, Op.type, parent);
            this.value = value;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return typeQuery(this.value);
        }
        //endregion

        //region Fields
        private String value;
        //endregion
    }

    public class ExistsComposite extends Composite {
        //region Constructor
        public ExistsComposite(String value, Composite parent) {
            super(null, Op.exists, parent);
            this.value = value;
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return existsQuery(this.value);
        }
        //endregion

        //region Fields
        private String value;
        //endregion
    }

    public class MatchAllComposite extends Composite {
        //region Constructor
        public MatchAllComposite(String name, Composite parent) {
            super(name, Op.matchAll, parent);
        }
        //endregion

        //region Composite Implementation
        @Override
        protected Object build() {
            return matchAllQuery();
        }
        //endregion
    }
    //endregion
}
