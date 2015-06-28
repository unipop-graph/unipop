package org.elasticgremlin.queryhandler;


public interface QueryHandler extends VertexHandler, EdgeHandler {

    void close();

    void clearAllData();
}
