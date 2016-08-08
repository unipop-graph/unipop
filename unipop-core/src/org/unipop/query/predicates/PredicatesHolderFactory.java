package org.unipop.query.predicates;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.*;
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
        return new PredicatesHolder(PredicatesHolder.Clause.And, Arrays.asList(predicates), null);
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

        List<HasContainer> predicates = new ArrayList<>();
        List<PredicatesHolder> children = new ArrayList<>();
        for(PredicatesHolder predicatesHolder : filteredPredicateHolders){
            if(predicatesHolder.getClause().equals(PredicatesHolder.Clause.And)){
                predicates.addAll(predicatesHolder.getPredicates());
                children.addAll(predicatesHolder.getChildren());
            }
            else children.add(predicatesHolder);
        }

        return new PredicatesHolder(PredicatesHolder.Clause.And, predicates, children);
    }

    public static PredicatesHolder or(HasContainer... predicates){
        return new PredicatesHolder(PredicatesHolder.Clause.Or, Arrays.asList(predicates), null);
    }

    public static PredicatesHolder or(PredicatesHolder... predicatesHolders) {
        return or(Sets.newHashSet(predicatesHolders));
    }
    public static PredicatesHolder or(Collection<PredicatesHolder> predicatesHolders) {
        if(predicatesHolders.size() == 0) return empty();

        List<PredicatesHolder> filteredPredicateHolders = predicatesHolders.stream()
                .filter(p -> p != null).filter(PredicatesHolder::notAborted).collect(Collectors.toList());
        if(filteredPredicateHolders.size() == 0) return abort();
        if(filteredPredicateHolders.size() == 1) return filteredPredicateHolders.iterator().next();

        return new PredicatesHolder(PredicatesHolder.Clause.Or, Collections.EMPTY_LIST, filteredPredicateHolders);
    }

    public static PredicatesHolder create(PredicatesHolder.Clause clause, Set<PredicatesHolder> predicatesHolders) {
        if(clause.equals(PredicatesHolder.Clause.And)) return and(predicatesHolders);
        if(clause.equals(PredicatesHolder.Clause.Or)) return or(predicatesHolders);
        return abort();
    }


    public static PredicatesHolder createFromPredicates(PredicatesHolder.Clause clause, Set<HasContainer> predicatesHolders) {
        return new PredicatesHolder(clause, new ArrayList<>(predicatesHolders), null);
    }
}
