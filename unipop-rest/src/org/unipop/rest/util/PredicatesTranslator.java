package org.unipop.rest.util;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 28/11/16.
 */
public class PredicatesTranslator {
    public static Map<String, Object> translate(PredicatesHolder predicatesHolder, JSONObject opTranslator, MatcherHolder complexTranslator, int limit) {
        Map<String, Object> translate = translate(predicatesHolder, opTranslator, complexTranslator);
        translate.put("limit", limit);
        return translate;
    }

    public static Map<String, Object> translate(PredicatesHolder predicatesHolder, JSONObject opTranslator, MatcherHolder complexTranslator) {
        List<HasContainer> predicates = predicatesHolder.getPredicates();
        Map<HasContainer, String> complexOps = predicates.stream().map(hasContainer -> {
            String match = complexTranslator.match(hasContainer);
            if (match != null)
                return Pair.of(hasContainer, match);
            return null;
        }).filter(t -> t != null).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        List<Map<String, Object>> predicatesMaps = predicates.stream().filter(has -> !complexOps.containsKey(has)).map(has -> {
            // TODO: maybe implement a simple matcher for op translator
            HashMap<String, Object> map = new HashMap<>();
            String[] split = has.getKey().split("\\.");
            map.put("key", split[split.length - 1]);
            Object value = has.getValue();
            if (value instanceof String)
                map.put("value", "\"" + value + "\"");
            else if (value instanceof Collection) {
                String finalString = "[%s]";
                List<String> values = ((Collection<Object>) value).stream().map(v -> "\"" + v.toString() + "\"").collect(Collectors.toList());
                String valuesString = "";
                String join = valuesString.join(",", values);
                map.put("value", String.format(finalString, join));
            } else
                map.put("value", value);
            if (!opTranslator.has(has.getBiPredicate().toString())) return null;
            map.put("op", opTranslator.getString(has.getBiPredicate().toString()));
            return map;
        }).filter(m -> m != null).collect(Collectors.toList());

        List<Map<String, Object>> children = predicatesHolder.getChildren().stream()
                .map(p -> translate(p, opTranslator, complexTranslator)).collect(Collectors.toList());

        Map<String, Object> map = new HashMap<>();
        Map<String, Object> predicateChildrenMap = new HashMap<>();
        predicateChildrenMap.put("predicates", predicatesMaps);
        predicateChildrenMap.put("complex", complexOps.values());
        if (children.size() > 0)
            predicateChildrenMap.put("children", children);
        map.put(predicatesHolder.getClause().toString().toLowerCase(), predicateChildrenMap);

        Map<String, Object> result = new HashMap<>();
        result.put("predicates", map);

        return result;
    }
}
