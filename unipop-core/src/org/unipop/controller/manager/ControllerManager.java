package org.unipop.controller.manager;

import org.unipop.controller.ElementController;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface ControllerManager{

    Set<ElementController> getControllers();

    default <T extends ElementController> List<T> getControllers(Class<? extends T> c){
        return getControllers().stream()
                .filter(controller -> c.isAssignableFrom(controller.getClass()))
                .map(controller -> (T) controller)
                .collect(Collectors.toList());
    }

    void close();
}
