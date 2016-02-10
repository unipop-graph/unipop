package org.unipop.elastic.controller.template.helpers;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.unipop.controller.ExistsP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateHelper {
    public static Map<String, Object> createTemplateParams(List<HasContainer> hasContainers) {
        Map<String, Object> params = new HashMap<>();

        Map<String, Object> filters = new HashMap<>();

        hasContainers.forEach(has -> addParam(filters, has));

        params.put("filters", filters);

        return params;
    }

    @SuppressWarnings("unchecked")
    private static void addOrCreate(Map<String, Object> params, String bool, String key, Map<String, Object> map) {
        if (params.containsKey(bool)) {
            Map<String, ArrayList<Map<String, Object>>> boolList = ((Map<String, ArrayList<Map<String, Object>>>) params.get(bool));
            if (boolList.containsKey(key))
                boolList.get(key).add(map);
            else {
                ArrayList<Map<String, Object>> arr = new ArrayList<>();
                arr.add(map);
                boolList.put(key, arr);
            }
        } else {
            ArrayList<Map<String, Object>> arr = new ArrayList<>();
            arr.add(map);
            Map<String, ArrayList<Map<String, Object>>> boolList = new HashMap<>();
            boolList.put(key, arr);
            params.put(bool, boolList);
        }
    }

    private static void addParam(Map<String, Object> params, HasContainer has) {
        String key = has.getKey();
        Object value = has.getValue();
        BiPredicate<?, ?> biPredicate = has.getBiPredicate();
        if (key.equals(T.id.getAccessor())) {
            if (value instanceof Iterable)
                params.put("ids", Iterables.toArray(((Iterable) value), Object.class));
            else
                params.put("ids",  value);
        } else if (key.equals(T.label.getAccessor())) {
            if (value instanceof Iterable)
                params.put("types", Iterables.toArray(((Iterable) value), Object.class));
            else
                params.put("types", value);
        } else if (biPredicate != null) {
            if (biPredicate instanceof Compare) {
                String predicateString = biPredicate.toString();
                switch (predicateString) {
                    case ("eq"):
                        Map<String, Object> eq = new HashMap<>();
                        eq.put("field", key);
                        eq.put("value", value);
                        addOrCreate(params, "must", "terms", eq);
                        break;
                    case ("neq"):
                        Map<String, Object> neq = new HashMap<>();
                        neq.put("field", key);
                        neq.put("value", value);
                        addOrCreate(params, "mustNot", "terms", neq);
                        break;
                    case ("gt"):
                        Map<String, Object> gt = new HashMap<>();
                        gt.put("field", key);
                        gt.put("value", value);
                        gt.put("func", predicateString);
                        addOrCreate(params, "must", "range", gt);
                        break;
                    case ("gte"):
                        Map<String, Object> gte = new HashMap<>();
                        gte.put("field", key);
                        gte.put("value", value);
                        gte.put("func", predicateString);
                        addOrCreate(params, "must", "range", gte);
                        break;
                    case ("lt"):
                        Map<String, Object> lt = new HashMap<>();
                        lt.put("field", key);
                        lt.put("value", value);
                        lt.put("func", predicateString);
                        addOrCreate(params, "must", "range", lt);
                        break;
                    case ("lte"):
                        Map<String, Object> lte = new HashMap<>();
                        lte.put("field", key);
                        lte.put("value", value);
                        lte.put("func", predicateString);
                        addOrCreate(params, "must", "range", lte);
                        break;
                    case ("inside"):
                        List items = (List) value;
                        Object firstItem = items.get(0);
                        Object secondItem = items.get(1);
                        Map<String, Object> greater = new HashMap<>();
                        greater.put("field", key);
                        greater.put("value", firstItem);
                        greater.put("func", predicateString);
                        addOrCreate(params, "must", "range", greater);
                        Map<String, Object> lower = new HashMap<>();
                        lower.put("field", key);
                        lower.put("value", secondItem);
                        lower.put("func", predicateString);
                        addOrCreate(params, "must", "range", lower);
                        break;
                    default:
                        throw new IllegalArgumentException("predicate not supported in has step: " + biPredicate.toString());
                }
            } else if (biPredicate instanceof Contains) {
                if (biPredicate == Contains.without) {
                    Map<String, Object> without = new HashMap<>();
                    without.put("field", key);
                    without.put("value", Iterables.toArray(((Iterable) value), Object.class));
                    addOrCreate(params, "mustNot", "terms", without);
                } else if (biPredicate == Contains.within) {
                    Map<String, Object> within = new HashMap<>();
                    within.put("field", key);
                    within.put("value", Iterables.toArray(((Iterable) value), Object.class));
                    addOrCreate(params, "must", "terms", within);
                }
            } else throw new IllegalArgumentException("predicate not supported by unipop: " + biPredicate.toString());
        } else throw new IllegalArgumentException("HasContainer not supported by unipop");
    }
}