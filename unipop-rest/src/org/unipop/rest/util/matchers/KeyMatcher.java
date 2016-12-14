package org.unipop.rest.util.matchers;

import com.samskivert.mustache.Template;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.rest.util.TemplateHolder;

import java.util.Map;

/**
 * Created by sbarzilay on 12/7/16.
 */
public class KeyMatcher implements Matcher {
    protected String key;
    protected Template template;

    public KeyMatcher(String key, Template template) {
        this.key = key;
        this.template = template;
    }

    @Override
    public boolean match(HasContainer hasContainer) {
        String[] split = hasContainer.getKey().split("\\.");
        return split[split.length - 1].equals(key);
    }

    @Override
    public String execute(HasContainer hasContainer) {
        Map<String, Object> stringObjectMap = toMap(hasContainer);
        return template.execute(stringObjectMap);
    }

    public static class KeyMatcherBuilder implements MatcherBuilder {
        @Override
        public Matcher build(JSONObject object) {
            if (object.has("key")){
                return new KeyMatcher(object.getString("key"), TemplateHolder.createTemplate(object.getString("template")));
            }
            return null;
        }
    }
}
