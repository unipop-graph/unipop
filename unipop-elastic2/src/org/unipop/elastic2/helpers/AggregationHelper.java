package org.unipop.elastic2.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.unipop.controller.aggregation.SemanticKeyTraversal;
import org.unipop.controller.aggregation.SemanticReducerTraversal;
import org.unipop.elastic2.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic2.controller.schema.helpers.aggregationConverters.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roman on 11/12/2015.
 */
public class AggregationHelper {

    public static HashMap<String, Pair<Long, Vertex>> getIdsCounts(Iterable<Vertex> vertices) {
        HashMap<String, Pair<Long, Vertex>> idsCount = new HashMap<>();
        vertices.forEach(vertex -> {
            Pair<Long, Vertex> pair;
            String id = vertex.id().toString();
            if (idsCount.containsKey(id)) {
                pair = idsCount.get(id);
                pair = new Pair<Long, Vertex>(pair.getValue0() + 1, vertex);
            } else {
                pair = new Pair<>(1L, vertex);
            }

            idsCount.put(id, pair);
        });

        return idsCount;
    }

    public static Long countResultsWithRespectToOriginalOccurrences(HashMap<String, Pair<Long, Vertex>> idsCount, Map<String, Object> result) {
        Long count = 0L;
        for (Map.Entry fieldAggregationEntry : result.entrySet()) {
            Map<String, Object> fieldAggregation = (Map<String, Object>)fieldAggregationEntry.getValue();

            for(Map.Entry entry : fieldAggregation.entrySet()) {
                Pair<Long, Vertex> vertexCountPair = idsCount.get(entry.getKey());
                if (vertexCountPair == null) {
                    continue;
                }

                Long occurrences = (Long)((Map<String, Object>)entry.getValue()).get("count");
                Long factor = vertexCountPair.getValue0();
                count += occurrences * factor;
            }
        }
        return count;
    }

    public static void applyAggregationBuilder(AggregationBuilder aggregationBuilder, Traversal keyTraversal, Traversal reducerTraversal, int aggSize, int shsrdSize, String executionHint) {
        if (SemanticKeyTraversal.class.isAssignableFrom(keyTraversal.getClass())) {
            SemanticKeyTraversal semanticKeyTraversal = (SemanticKeyTraversal) keyTraversal;
            aggregationBuilder.terms("key")
                    .field(semanticKeyTraversal.getKey())
                    .size(aggSize)
                    .shardSize(shsrdSize)
                    .executionHint(executionHint);

            if (reducerTraversal != null && SemanticReducerTraversal.class.isAssignableFrom(reducerTraversal.getClass())) {
                SemanticReducerTraversal semanticReducerTraversalInstance = (SemanticReducerTraversal)reducerTraversal;
                String reduceAggregationName = "reduce";
                switch (semanticReducerTraversalInstance.getType()) {
                    case count:
                        aggregationBuilder.count(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case min:
                        aggregationBuilder.min(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case max:
                        aggregationBuilder.max(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case cardinality:
                        aggregationBuilder.cardinality(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey())
                                .precisionThreshold(1000L);
                        break;
                }
            }
        }
    }

    public static MapAggregationConverter getAggregationConverter(AggregationBuilder aggregationBuilder, boolean useSimpleFormat) {

        MapAggregationConverter mapAggregationConverter = new MapAggregationConverter();

        FilteredMapAggregationConverter filteredMapAggregationConverter = new FilteredMapAggregationConverter(
                aggregationBuilder,
                mapAggregationConverter);

        FilteredMapAggregationConverter filteredStatsAggregationConverter = new FilteredMapAggregationConverter(
                aggregationBuilder,
                new StatsAggregationConverter()
        );

        CompositeAggregationConverter compositeAggregationConverter = new CompositeAggregationConverter(
                filteredMapAggregationConverter,
                filteredStatsAggregationConverter,
                new SingleValueAggregationConverter()
        );

        mapAggregationConverter.setInnerConverter(compositeAggregationConverter);
        mapAggregationConverter.setUseSimpleFormat(useSimpleFormat);
        return mapAggregationConverter;
    }
}
