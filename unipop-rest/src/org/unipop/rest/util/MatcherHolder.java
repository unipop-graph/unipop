package org.unipop.rest.util;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.unipop.rest.util.matchers.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by sbarzilay on 12/8/16.
 */
public class MatcherHolder {
    protected List<Matcher> matchers;

    public MatcherHolder(JSONObject configuration, List<Matcher.MatcherBuilder> builders) {
        matchers = new ArrayList<>();
        if(configuration.has("complexTranslator")){
            JSONArray complexTranslator = configuration.getJSONArray("complexTranslator");
            for (int i = 0; i < complexTranslator.length(); i++) {
                JSONObject jsonObject = complexTranslator.getJSONObject(i);
                Optional<Matcher> matcher = builders.stream().map(matcherBuilder -> matcherBuilder.build(jsonObject)).filter(m -> m != null).findFirst();
                if (matcher.isPresent())
                    matchers.add(matcher.get());
            }
        }
    }

    public String match(HasContainer hasContainer){
        for (Matcher matcher : matchers) {
            if (matcher.match(hasContainer))
                return matcher.execute(hasContainer);
        }
        return null;
    }
}
