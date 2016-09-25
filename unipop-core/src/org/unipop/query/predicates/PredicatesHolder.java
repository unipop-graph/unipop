package org.unipop.query.predicates;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PredicatesHolder {

    public enum Clause {
        And,
        Or,
        Abort
    }

    private Clause clause;
    private List<HasContainer> predicates;
    private List<PredicatesHolder> children;

    public PredicatesHolder(Clause clause, List<HasContainer> predicates, List<PredicatesHolder> children) {
        this.clause = clause;
        this.predicates = predicates != null ? predicates : Collections.emptyList();
        this.children = children != null ? children : Collections.emptyList();
    }

    public PredicatesHolder.Clause getClause() {
        return this.clause;
    }

    public List<HasContainer> getPredicates() {
        return predicates;
    }

    public List<PredicatesHolder> getChildren() {
        return children;
    }

    public boolean hasPredicates() {
        return getPredicates().size() > 0;
    }

    public boolean hasChildren() {
        return getChildren().size() > 0;
    }

    public boolean isEmpty() {
        return !hasPredicates() && !hasChildren();
    }

    public boolean notEmpty() {
        return !isEmpty();
    }

    public boolean isAborted() {
        return this.clause.equals(Clause.Abort);
    }

    public boolean notAborted() {
        return !isAborted();
    }

    public Stream<HasContainer> findKey(String key) {
        return predicates.stream().filter(has -> has.getKey().equals(key));
    }

//    public <E extends Element> boolean test(E element) {
//        if(getClause().equals(Clause.And)) {
//            if (!HasContainer.testAll(element, this.predicates)) return false;
//
//            for (PredicatesHolder child : this.children) {
//                if (!child.test(element)) return false;
//            }
//            return true;
//        }
//        else {
//            for(HasContainer has : this.predicates) {
//                if (has.test(element)) return true;
//            }
//            for (PredicatesHolder child : this.children) {
//                if (child.test(element)) return true;
//            }
//            return false;
//        }
//    }

    public PredicatesHolder map(Function<HasContainer, HasContainer> func){
        List<HasContainer> predicates = getPredicates().stream().map(func).collect(Collectors.toList());
        List<PredicatesHolder> children = getChildren().stream().map(child -> child.map(func)).collect(Collectors.toList());
        return new PredicatesHolder(getClause(), predicates,children);
    }
}
