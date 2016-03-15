package org.unipop.controller.provider;

import org.apache.commons.configuration.Configuration;
import org.unipop.controller.Controller;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public abstract class ControllerProvider {

    private Set<Controller> controllers = new HashSet<>();

    public abstract void init(UniGraph graph, Configuration configuration) throws Exception;

    protected void addController(Controller controller) {
        controllers.add(controller);
    }

    public Set<Controller> getControllers() {
        return controllers;
    }

    public <T extends Controller> Stream<T> getControllers(Class<? extends T> c){
        return controllers.stream()
                .filter(controller -> c.isAssignableFrom(controller.getClass()))
                .map(controller -> (T) controller);
    }
}
