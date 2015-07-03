package org.elasticgremlin.queryhandler;


import org.apache.commons.configuration.Configuration;
import org.elasticgremlin.structure.ElasticGraph;

import java.io.IOException;

public interface QueryHandler extends VertexHandler, EdgeHandler {

    void init(ElasticGraph graph, Configuration configuration) throws IOException;
    void commit();
    void close();
}
