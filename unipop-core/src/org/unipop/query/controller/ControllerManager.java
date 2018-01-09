package org.unipop.query.controller;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A controller manager hold all of your controllers
 */
public interface ControllerManager {
    /**
     * Returns all of the controllers
     * @return A set of all the controllers
     */
    Set<UniQueryController> getControllers();

    /**
     * Returns all controllers of class T
     * @param c Controller's class
     * @param <T> Extends UniQueryController
     * @return A list of controllers the type of c
     */
    default <T extends UniQueryController> List<T> getControllers(Class<? extends T> c){
        return getControllers().stream()
                .filter(controller -> c.isAssignableFrom(controller.getClass()))
                .map(controller -> (T) controller)
                .collect(Collectors.toList());
    }

    /**
     * Closes all controllers
     */
    void close();
}
