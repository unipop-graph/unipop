package org.unipop.elastic.controller.template.helpers;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateHelper {
    public static Map<String, Object> createTemplateParams(List<HasContainer> hasContainers, Map<String, String> defaultParams) {
        Map<String, Object> params = new HashMap<>();

        hasContainers.forEach(has -> {
            if (has.getValue() instanceof Iterable)
                params.put(defaultParams.getOrDefault(has.getKey(), has.getKey()), Iterables.toArray((Iterable) has.getValue(), Object.class));
            else
                params.put(defaultParams.getOrDefault(has.getKey(), has.getKey()), has.getValue());
        });

        return params;
    }

    public static Map<String, Object> createTemplateParams(List<HasContainer> hasContainers){
        return createTemplateParams(hasContainers, new HashMap<>());
    }
}