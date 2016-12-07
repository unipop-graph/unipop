package org.unipop.rest.util.matchers;

import com.samskivert.mustache.Template;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.rest.util.TemplateHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by sbarzilay on 12/8/16.
 */
public class MultiOpMatcher implements Matcher {
    protected List<String> opsList;
    protected Template template;

    public MultiOpMatcher(List<String> opsList, Template template) {
        this.opsList = opsList;
        this.template = template;
    }

    @Override
    public boolean match(HasContainer hasContainer) {
        return opsList.contains(hasContainer.getBiPredicate().toString());
    }

    @Override
    public String execute(HasContainer hasContainer) {
        Map<String, Object> stringObjectMap = toMap(hasContainer);
        stringObjectMap.put("op", hasContainer.getBiPredicate().toString());
        return template.execute(stringObjectMap);
    }

    public static class MultiOpMatcherBuilder implements MatcherBuilder {
        @Override
        public Matcher build(JSONObject object) {
            if (object.has("ops")){
                JSONArray ops = object.getJSONArray("ops");
                List<String> opsList = new ArrayList<>();
                for (int i = 0; i < ops.length(); i++) {
                    opsList.add(ops.getString(i));
                }
                return new MultiOpMatcher(opsList, TemplateHolder.createTemplate(object.getString("template")));
            }
            return null;        }
    }
}
