package org.elasticgremlin.querying;


public interface QueryHandler extends VertexHandler, EdgeHandler {

    void close();

    void clearAllData();
}
