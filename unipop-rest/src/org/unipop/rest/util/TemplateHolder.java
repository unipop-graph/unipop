package org.unipop.rest.util;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;

/**
 * Created by sbarzilay on 5/12/16.
 */
public class TemplateHolder {
    private TemplateRequest search;
    private TemplateRequest add;
    private TemplateRequest delete;
    private TemplateRequest commit;
    private TemplateRequest bulk;

    private static Reader getReader(String mustache) {
        if (mustache.contains("{"))
            return new StringReader(mustache);
        try {
            return new FileReader(mustache);
        } catch (FileNotFoundException e) {
            return new StringReader(mustache);
        }
    }

    public static Template createTemplate(String mustache) {
        return Mustache.compiler().escapeHTML(false).compile(getReader(mustache));
    }

    public TemplateHolder(JSONObject configuration) {
        JSONObject search = configuration.optJSONObject("search");
        String searchString = search.getString("template");
        Template searchTemplate = Mustache.compiler().escapeHTML(false).standardsMode(true).withLoader(s ->
                getReader(searchString)).compile(getReader(searchString));
        Template searchUrlTemplate = createTemplate(search.optString("url", ""));
        String searchMethod = search.optString("method", "POST");
        String field = null;
        if (searchMethod.toUpperCase().equals("GET"))
            field = search.optString("field");
        this.search = new TemplateRequest(searchMethod, searchUrlTemplate, searchTemplate, field);
        JSONObject add = configuration.optJSONObject("add");
        JSONObject delete = configuration.optJSONObject("delete");
        if (add != null) {
            this.add = new TemplateRequest(add.optString("method", "POST"),
                    createTemplate(add.optString("url")),
                    createTemplate(add.optString("template")));
            JSONObject bulk = add.optJSONObject("bulk");
            JSONObject commit = add.optJSONObject("commit");
            if (commit != null)
                this.commit = new TemplateRequest(commit.optString("method", "GET"),
                        createTemplate(commit.optString("url")),
                        createTemplate(commit.optString("template")));
            if (bulk != null) {
                Template bulkUrlTemplate = createTemplate(bulk.optString("url"));
                Template bulkTemplate = Mustache.compiler().escapeHTML(false).standardsMode(true).withLoader(s ->
                        getReader(add.optString("template"))).compile(getReader(bulk.optString("template")));
                this.bulk = new TemplateRequest(bulk.optString("method", "POST"), bulkUrlTemplate, bulkTemplate);
            }
        }

        if (delete != null)
            this.delete = new TemplateRequest(delete.optString("method", "DELETE"),
                    createTemplate(delete.optString("url")),
                    createTemplate(delete.optString("body")));
    }

    public TemplateRequest getSearch() {
        return search;
    }

    public TemplateRequest getAdd() {
        return add;
    }

    public TemplateRequest getDelete() {
        return delete;
    }

    public TemplateRequest getCommit() {
        return commit;
    }

    public TemplateRequest getBulk() {
        return bulk;
    }

    public boolean isBulk() {
        return bulk != null;
    }

    public boolean isAdd() {
        return add != null;
    }

    public boolean isCommit() {
        return commit != null;
    }

    public boolean isDelete() {
        return delete != null;
    }
}
