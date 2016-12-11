package org.unipop.rest.util;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.BaseRequest;
import com.mashape.unirest.request.HttpRequestWithBody;
import com.samskivert.mustache.Template;

import java.util.Map;

/**
 * Created by sbarzilay on 11/12/16.
 */
public class TemplateRequest {
    protected String method;
    protected Template urlTemplate;
    protected Template bodyTemplate;

    public TemplateRequest(String method, Template urlTemplate, Template bodyTemplate) {
        this.method = method;
        this.urlTemplate = urlTemplate;
        this.bodyTemplate = bodyTemplate;
    }

    public BaseRequest execute(String baseUrl, Object urlMap, Object bodyMap){
        switch (method.toUpperCase()){
            case "GET":
                return executeGet(baseUrl, urlMap, bodyMap);
            case "POST":
                return executePost(baseUrl, urlMap, bodyMap);
            case "PUT":
                return executePut(baseUrl, urlMap, bodyMap);
            case "PATCH":
                return executePatch(baseUrl, urlMap, bodyMap);
            case "DELETE":
                return executeDelete(baseUrl, urlMap, bodyMap);
            default:
                throw new RuntimeException("Method: " + method + " not supported");
        }
    }

    public boolean exists(){
        return urlTemplate != null;
    }

    private BaseRequest executeDelete(String baseUrl, Object urlMap, Object bodyMap) {
        HttpRequestWithBody delete = Unirest.delete(baseUrl + urlTemplate.execute(urlMap));
        if (bodyTemplate != null)
            return delete.body(bodyTemplate.execute(bodyMap));
        return delete;
    }

    private BaseRequest executePatch(String baseUrl, Object urlMap, Object bodyMap) {
        return Unirest.patch(baseUrl + urlTemplate.execute(urlMap)).body(bodyTemplate.execute(bodyMap));
    }

    private BaseRequest executePut(String baseUrl, Object urlMap, Object bodyMap) {
        return Unirest.put(baseUrl + urlTemplate.execute(urlMap)).body(bodyTemplate.execute(bodyMap));
    }

    private BaseRequest executePost(String baseUrl, Object urlMap, Object bodyMap) {
        return Unirest.post(baseUrl + urlTemplate.execute(urlMap)).body(bodyTemplate.execute(bodyMap));
    }

    private BaseRequest executeGet(String baseUrl, Object urlMap, Object bodyMap) {
        return Unirest.get(baseUrl + urlTemplate.execute(urlMap) + bodyTemplate.execute(bodyMap));
    }
}
