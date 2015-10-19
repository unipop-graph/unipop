package org.unipop.jdbc.basic;

import org.apache.commons.configuration.Configuration;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.structure.UniGraph;

import java.io.IOException;

public class BasicJdbcControllerManager extends BasicControllerManager {

    private EdgeController edgeController;
    private VertexController vertexController;


    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException {

    }

    @Override
    protected VertexController getDefaultVertexController() {
        return vertexController;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return edgeController;
    }

    @Override
    public void commit() {}

    @Override
    public void close() {

    }

    @Override
    public void printStats() {

    }
}
