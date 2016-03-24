package org.unipop.manager;

import org.unipop.structure.manager.ControllerProvider;

/**
 * Created by sbarzilay on 2/12/16.
 */
public class JSONControllerManagerFactory implements ControllerManagerFactory {
    @Override
    public ControllerProvider getControllerManager() {
        return new JSONSchemaControllerManager();
    }
}
