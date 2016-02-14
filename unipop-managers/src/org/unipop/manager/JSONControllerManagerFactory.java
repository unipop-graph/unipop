package org.unipop.manager;

import org.unipop.controllerprovider.ControllerManager;
import org.unipop.controllerprovider.ControllerManagerFactory;

/**
 * Created by sbarzilay on 2/12/16.
 */
public class JSONControllerManagerFactory implements ControllerManagerFactory {
    @Override
    public ControllerManager getControllerManager() {
        return new JSONSchemaControllerManager();
    }
}
