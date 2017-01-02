package org.unipop.rest.util.matchers;

import com.samskivert.mustache.Template;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.rest.util.TemplateHolder;

import java.util.Map;

/**
 * Created by sbarzilay on 12/7/16.
 */
public class OpMatcher implements Matcher {
    protected String operator;
    protected Template template;

    public OpMatcher(String operator, Template template) {
        this.operator = operator;
        this.template = template;
    }

    @Override
    public boolean match(HasContainer hasContainer) {
        return hasContainer.getBiPredicate().toString().equals(operator);
    }

    @Override
    public String execute(HasContainer hasContainer) {
        Map<String, Object> stringObjectMap = toMap(hasContainer);
        return template.execute(stringObjectMap);
    }

    public static class OpMatcherBuilder implements MatcherBuilder {
        @Override
        public Matcher build(JSONObject object) {
            if (object.has("op")){
                return new OpMatcher(object.getString("op"), TemplateHolder.createTemplate(object.getString("template")));
            }
            return null;
        }
    }
}
