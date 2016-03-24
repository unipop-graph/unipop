package org.unipop.structure.manager;

import org.unipop.controller.Controller;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface ControllerManager{

    Set<Controller> getControllers();

    default <T extends Controller> List<T> getControllers(Class<? extends T> c){
        return getControllers().stream()
                .filter(controller -> c.isAssignableFrom(controller.getClass()))
                .map(controller -> (T) controller)
                .collect(Collectors.toList());
    }

    void close();
}
