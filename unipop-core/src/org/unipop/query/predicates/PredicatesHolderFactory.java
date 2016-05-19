package org.unipop.query.predicates;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

//TODO: smarter merge: by predicate key
public class PredicatesHolderFactory {

    public static PredicatesHolder abort() {
        return new PredicatesHolder(PredicatesHolder.Clause.Abort, null, null);
    }

    public static PredicatesHolder empty() {
        return new PredicatesHolder(PredicatesHolder.Clause.And, null, null);
    }

    public static PredicatesHolder predicate(HasContainer predicate) {
        return and(predicate);
    }

    public static PredicatesHolder and(HasContainer... predicates) {
        return new PredicatesHolder(PredicatesHolder.Clause.And, Sets.newHashSet(predicates), null);
    }

    public static PredicatesHolder and(PredicatesHolder... predicatesHolders) {
        return and(Sets.newHashSet(predicatesHolders));
    }

    public static PredicatesHolder and(Set<PredicatesHolder> predicatesHolders) {
        if(predicatesHolders.stream().filter(PredicatesHolder::isAborted).count() > 0) return abort();

        Set<PredicatesHolder> filteredPredicateHolders = predicatesHolders.stream()
                .filter(PredicatesHolder::notEmpty).collect(Collectors.toSet());
        if(filteredPredicateHolders.size() == 0) return empty();
        if(filteredPredicateHolders.size() == 1) return filteredPredicateHolders.iterator().next();

        Set<HasContainer> predicates = new HashSet<>();
        Set<PredicatesHolder> children = new HashSet<>();
        for(PredicatesHolder predicatesHolder : filteredPredicateHolders){
            if(predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)){
                predicates.addAll(predicatesHolder.getPredicates());
                children.addAll(predicatesHolder.getChildren());
            }
            else children.add(predicatesHolder);
        }

        return new PredicatesHolder(PredicatesHolder.Clause.And, predicates, children);
    }

    public static PredicatesHolder or(PredicatesHolder... predicatesHolders) {
        return or(Sets.newHashSet(predicatesHolders));
    }
    public static PredicatesHolder or(Set<PredicatesHolder> predicatesHolders) {
        if(predicatesHolders.size() == 0) return empty();

        Set<PredicatesHolder> filteredPredicateHolders = predicatesHolders.stream()
                .filter(PredicatesHolder::notAborted).collect(Collectors.toSet());
        if(filteredPredicateHolders.size() == 0) return abort();
        if(filteredPredicateHolders.size() == 1) return predicatesHolders.iterator().next();

        return new PredicatesHolder(PredicatesHolder.Clause.Or, Collections.EMPTY_SET, filteredPredicateHolders);
    }

    public static PredicatesHolder create(PredicatesHolder.Clause clause, Set<PredicatesHolder> predicatesHolders) {
        if(clause.equals(PredicatesHolder.Clause.And)) return and(predicatesHolders);
        if(clause.equals(PredicatesHolder.Clause.Or)) return or(predicatesHolders);
        return abort();
    }
}
