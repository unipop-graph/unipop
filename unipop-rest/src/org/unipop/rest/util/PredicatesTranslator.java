package org.unipop.rest.util;

import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 28/11/16.
 */
public class PredicatesTranslator {
    public static Map<String, Object> translate(PredicatesHolder predicatesHolder, JSONObject opTranslator, int limit) {
        Map<String, Object> translate = translate(predicatesHolder, opTranslator);
        translate.put("limit", limit);
        return translate;
    }

    public static Map<String, Object> translate(PredicatesHolder predicatesHolder, JSONObject opTranslator) {
        List<Map<String, Object>> predicatesMaps = predicatesHolder.getPredicates().stream().map(has -> {
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
                .map(p -> translate(p, opTranslator)).collect(Collectors.toList());

        Map<String, Object> map = new HashMap<>();
        Map<String, Object> predicateChildrenMap = new HashMap<>();
        predicateChildrenMap.put("predicates", predicatesMaps);
        if (children.size() > 0)
            predicateChildrenMap.put("children", children);
        map.put(predicatesHolder.getClause().toString().toLowerCase(), predicateChildrenMap);

        Map<String, Object> result = new HashMap<>();
        result.put("predicates", map);
        return result;
    }
}
