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
    private Template searchUrlTemplate;
    private Template searchTemplate;
    private Template addTemplate;
    private Template addUrlTemplate;
    private Template deleteUrlTemplate;
    private Template commitUrlTemplate;
    private Template bulkUrlTemplate;
    private Template bulkTemplate;

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
        return Mustache.compiler().compile(getReader(mustache));
    }

    public TemplateHolder(JSONObject configuration) {
        JSONObject search = configuration.optJSONObject("search");
        JSONObject add = configuration.optJSONObject("add");
        JSONObject delete = configuration.optJSONObject("delete");
        if (add != null) {
            JSONObject bulk = add.optJSONObject("bulk");
            this.addUrlTemplate = createTemplate(add.optString("url"));
            this.addTemplate = createTemplate(add.optString("template"));
            this.commitUrlTemplate = createTemplate(add.optString("commit"));
            if (bulk != null) {
                this.bulkUrlTemplate = createTemplate(bulk.optString("url"));
                this.bulkTemplate = Mustache.compiler().escapeHTML(false).standardsMode(true).withLoader(s ->
                        getReader(add.optString("template"))).compile(getReader(bulk.optString("template")));
            }
        }
        String searchString = search.getString("template");
        this.searchTemplate = Mustache.compiler().escapeHTML(false).standardsMode(true).withLoader(s ->
                getReader(searchString)).compile(getReader(searchString));
        this.searchUrlTemplate = createTemplate(search.optString("url", ""));
        if (delete != null)
            this.deleteUrlTemplate = createTemplate(delete.optString("url"));
    }

    public Template getSearchUrlTemplate() {
        return searchUrlTemplate;
    }

    public Template getSearchTemplate() {
        return searchTemplate;
    }

    public Template getAddTemplate() {
        return addTemplate;
    }

    public Template getAddUrlTemplate() {
        return addUrlTemplate;
    }

    public Template getDeleteUrlTemplate() {
        return deleteUrlTemplate;
    }

    public Template getCommitUrlTemplate() {
        return commitUrlTemplate;
    }

    public Template getBulkUrlTemplate() {
        return bulkUrlTemplate;
    }

    public Template getBulkTemplate() {
        return bulkTemplate;
    }

    public boolean isBulk() {
        return bulkTemplate != null;
    }

    public boolean isAdd() {
        return addUrlTemplate != null;
    }

    public boolean isCommit() {
        return commitUrlTemplate != null;
    }

    public boolean isDelete() {
        return deleteUrlTemplate != null;
    }
}