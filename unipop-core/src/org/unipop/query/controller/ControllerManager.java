package org.unipop.query.controller;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface ControllerManager {

    Set<UniQueryController> getControllers();

    default <T extends UniQueryController> List<T> getControllers(Class<? extends T> c){
        return getControllers().stream()
                .filter(controller -> c.isAssignableFrom(controller.getClass()))
                .map(controller -> (T) controller)
                .collect(Collectors.toList());
    }

    void close();
}
