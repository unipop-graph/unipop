package org.unipop.rest.util.matchers;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 12/7/16.
 */
public interface Matcher {
    boolean match(HasContainer hasContainer);
    String execute(HasContainer hasContainer);

    default Map<String, Object> toMap(HasContainer hasContainer) {
        HashMap<String, Object> map = new HashMap<>();
        String[] split = hasContainer.getKey().split("\\.");
        map.put("key", split[split.length - 1]);
        Object value = hasContainer.getValue();
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
        return map;
    }

    interface MatcherBuilder {
        Matcher build(JSONObject object);
    }
}
