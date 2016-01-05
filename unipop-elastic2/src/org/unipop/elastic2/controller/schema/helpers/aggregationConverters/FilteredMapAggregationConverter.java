package org.unipop.elastic2.controller.schema.helpers.aggregationConverters;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.elasticsearch.search.aggregations.Aggregation;
import org.unipop.elastic2.controller.schema.helpers.AggregationBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 8/2/2015.
 */
public class FilteredMapAggregationConverter implements AggregationConverter<Aggregation, Map<String, Object>> {
    //region Constructor
    public FilteredMapAggregationConverter(
            AggregationBuilder aggregationBuilder,
            AggregationConverter<Aggregation, Map<String, Object>> innerConverter) {
        this.aggregationBuilder = aggregationBuilder;
        this.innerConverter = innerConverter;
    }
    //endregion

    //region AggregationConverter Implementation
    @Override
    public boolean canConvert(Aggregation aggregation) {
        return this.innerConverter.canConvert(aggregation);
    }

    @Override
    public Map<String, Object> convert(Aggregation aggregation) {
        Map<String, Object> map = this.innerConverter.convert(aggregation);

        List<HavingCompositeMatching> compositeMatchings = this.aggregationBuilder.find(composite ->
                composite.getParent() != null && composite.getParent().getName() == aggregation.getName()).stream()
                .filter(composite -> AggregationBuilder.HavingComposite.class.isAssignableFrom(composite.getClass()))
                .map(composite -> new HavingCompositeMatching((AggregationBuilder.HavingComposite) composite)).collect(Collectors.toList());

        if (compositeMatchings.size() > 0) {
            filterMap(map, compositeMatchings);
        }

        return map;
    }
    //endregion

    //region Private Methods
    private void filterMap(Map<String, Object> map, Iterable<HavingCompositeMatching> compositeMatchings) {
        int maxPathLength = StreamSupport.stream(compositeMatchings.spliterator(), false).map(compositeMatching -> compositeMatching.getPath().length).max(Integer::max).get();

        String[] path = new String[maxPathLength];
        List<String> keysToDelete = new ArrayList<>();

        for(Map.Entry<String, Object> entry : map.entrySet()) {
            compositeMatchings.forEach(compositeMatching -> compositeMatching.setIsMatched(false));

            path[0] = entry.getKey();

            boolean shouldFilterKey = shouldFilterKey(path, 1, entry.getValue(), compositeMatchings);
            boolean anyUnmatchedHaving = StreamSupport.stream(compositeMatchings.spliterator(), false).anyMatch(compositeMatching -> !compositeMatching.getIsMatched());

            if (shouldFilterKey || anyUnmatchedHaving) {
                keysToDelete.add(entry.getKey());
            }
        }

        for (String keyToDelete : keysToDelete) {
            map.remove(keyToDelete);
        }
    }

    private boolean shouldFilterKey(String[] path, int pathLength, Map<String, Object> map, Iterable<HavingCompositeMatching> compositeMatchings) {
        if (path.length <= pathLength) {
            return false;
        }

        for(Map.Entry<String, Object> entry : map.entrySet()) {
            path[pathLength] = entry.getKey();

            boolean shouldFilterKey = shouldFilterKey(path, pathLength + 1, entry.getValue(), compositeMatchings);
            if (shouldFilterKey) {
                return true;
            }
        }

        return false;
    }

    private boolean shouldFilterKey(String[] path, int pathLength, Object value, Iterable<HavingCompositeMatching> compositeMatchings) {
        if (Map.class.isAssignableFrom(value.getClass())) {
            boolean shouldFilterKey = shouldFilterKey(path, pathLength, (Map<String, Object>) value, compositeMatchings);
            if (shouldFilterKey) {
                return true;
            }
        }

        for (HavingCompositeMatching compositeMatching : StreamSupport.stream(compositeMatchings.spliterator(), false)
                    .filter(compositePath -> compositePath.isMatchingPath(path, pathLength)).collect(Collectors.toList())) {
            compositeMatching.setIsMatched(true);
            if (!testCondition(compositeMatching, value)) {
                return true;
            }
        }

        return false;
    }

    private boolean testCondition(HavingCompositeMatching compositeMatching, Object value) {
        HasContainer hasContainer = compositeMatching.getHavingComposite().getHasContainer();

        String specialPart = compositeMatching.getSpecialPart();
        if (specialPart != null) {
            switch (specialPart) {
                case "$size":
                    if (!Map.class.isAssignableFrom(value.getClass())) {
                        return false;
                    }

                    return ((BiPredicate<Long, Long>)hasContainer.getBiPredicate()).test((long) ((Map) value).size(), (long)hasContainer.getValue());
            }
        }

        return ((BiPredicate<Object, Object>)hasContainer.getPredicate()).test(value, hasContainer.getValue());
    }
    //endregion

    //region Fields
    protected AggregationConverter<Aggregation, Map<String, Object>> innerConverter;
    protected AggregationBuilder aggregationBuilder;
    //endregion

    //region HavingCompositePath
    private class HavingCompositeMatching {
        //region Constructor
        public HavingCompositeMatching(AggregationBuilder.HavingComposite havingComposite) {
            this.havingComposite = havingComposite;
            this.path = havingComposite.getHasContainer().getKey().split("\\.");

            String lastPart = this.path[this.path.length - 1];
            if (lastPart.startsWith("$")) {
                this.path = Arrays.copyOfRange(this.path, 0, this.path.length - 1);
                this.specialPart = lastPart;
            }
        }
        //endregion

        //region Public Methods
        public boolean isMatchingPath(String[] path, int length) {
            if (length != this.getPath().length) {
                return false;
            }

            if (path == null && this.path != null) {
                return false;
            }

            if (path != null && this.path == null) {
                return false;
            }

            for(int index = 0; index < length ; index++) {
                String part = path[index];
                String thisPart = this.path[index];

                if (thisPart.equals("*") || part.equals("*")) {
                    continue;
                }

                if (part == null && thisPart != null) {
                    return false;
                }

                if (part != null && thisPart == null) {
                    return false;
                }

                if (!part.equals(thisPart)) {
                    return false;
                }
            }

            return true;
        }
        //endregion

        //region Properties
        public AggregationBuilder.HavingComposite getHavingComposite() {
            return this.havingComposite;
        }

        public String[] getPath() {
            return this.path;
        }

        public boolean getIsMatched() {
            return this.isMatched;
        }

        public void setIsMatched(boolean value) {
            this.isMatched = value;
        }

        public String getSpecialPart() {
            return this.specialPart;
        }
        //endregion

        //region Fields
        protected AggregationBuilder.HavingComposite havingComposite;
        protected String[] path;
        protected boolean isMatched;
        protected String specialPart;
        //endregion
    }
    //endregion
}
