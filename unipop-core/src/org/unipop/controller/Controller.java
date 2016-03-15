package org.unipop.controller;

import org.unipop.structure.UniGraph;

import java.util.Map;

public interface Controller {
    void init(Map<String, Object> conf, UniGraph graph) throws Exception;
    void commit();
    void close();
}
