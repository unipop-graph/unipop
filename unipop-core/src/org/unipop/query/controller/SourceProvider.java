package org.unipop.query.controller;

import org.json.JSONObject;
import org.unipop.structure.UniGraph;

import java.util.Set;

public interface SourceProvider {
    Set<UniQueryController> init(UniGraph graph, JSONObject configuration) throws Exception;
    void close();
}
