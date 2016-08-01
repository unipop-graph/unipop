package org.unipop.schema.property;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 7/28/16.
 */
public class ArrayPropertySchema implements PropertySchema {
    private String key;
    private List<String> fields;

    public ArrayPropertySchema(String key, List<String> fields) {
        this.key = key;
        this.fields = fields;
    }

    @Override
    public Map<String, Object> toProperties(Map<String, Object> source) {
        List<Object> value = new ArrayList<>();
        fields.forEach(field -> {
            if (field.startsWith("@"))
                value.add(source.get(field.substring(1)));
            else
                value.add(field);
        });
        return Collections.singletonMap(key, value);
    }

    @Override
    public Set<String> excludeDynamicFields() {
        return new HashSet<>(this.fields);
    }

    @Override
    public Set<String> excludeDynamicProperties() {
        return Collections.singleton(this.key);
    }

    @Override
    public Map<String, Object> toFields(Map<String, Object> properties) {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return propertyKeys.contains(key) ? fields.stream()
                .filter(s -> s.startsWith("@")).map(s -> s.substring(1)).collect(Collectors.toSet()) :
                Collections.emptySet();
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        Stream<HasContainer> hasContainers = predicatesHolder.findKey(this.key);

        Set<PredicatesHolder> predicateHolders = hasContainers.map(this::toPredicate).collect(Collectors.toSet());
        return PredicatesHolderFactory.create(predicatesHolder.getClause(), predicateHolders);
    }

    private PredicatesHolder toPredicate(HasContainer hasContainer) {
        Object value = hasContainer.getValue();
        Set<HasContainer> predicates = new HashSet<>();
        if (value instanceof Collection) {
            for (String field : fields) {
                if (field.startsWith("@")) {
                    for (Object v : ((Collection) value)) {
                        HasContainer clone = new HasContainer(field.substring(1), new P(hasContainer.getBiPredicate(), v));
                        predicates.add(clone);
                    }
                } else {
                    if (new P(hasContainer.getBiPredicate(), hasContainer.getValue()).test(field)) {
                        return PredicatesHolderFactory.empty();
                    }
                }
            }
        } else {
            final boolean[] abort = {false};
            fields.forEach(field -> {
                if (field.startsWith("@")) {
                    HasContainer clone = hasContainer.clone();
                    clone.setKey(field.substring(1));
                    predicates.add(clone);
                } else {
                    if (new P(hasContainer.getBiPredicate(), hasContainer.getValue()).test(field)) {
                        abort[0] = true;
                        return;
                    }
                }
            });
            if (abort[0])
                return PredicatesHolderFactory.empty();
        }
        return PredicatesHolderFactory.or(predicates.toArray(new HasContainer[predicates.size()]));
    }
}
