package org.unipop.controller.manager;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.unipop.controller.ElementController;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConfigurationControllerManager implements ControllerManager {

    protected Set<ControllerProvider> controllerProviders = new HashSet<>();
    protected Set<ElementController> controllers = new HashSet<>();

    public ConfigurationControllerManager(UniGraph graph, Configuration configuration) throws Exception {
        HierarchicalConfiguration hierarchicalConfiguration = ConfigurationUtils.convertToHierarchical(configuration);
        List<HierarchicalConfiguration> controllerProviderConfigurations = hierarchicalConfiguration.configurationsAt("mappings");
        if(controllerProviderConfigurations.size() == 0) throw new InstantiationException("No 'mappings' configured for ConfigurationControllerManager");


        for(HierarchicalConfiguration cpConfiguration : controllerProviderConfigurations) {
            String builderName = cpConfiguration.getString("class");
            ControllerProvider controllerProvider = Class.forName(builderName).asSubclass(ControllerProvider.class).newInstance();
            controllerProviders.add(controllerProvider);
            controllerProvider.init(graph, cpConfiguration).forEach(this.controllers::add);
        }
    }

    @Override
    public Set<ElementController> getControllers() {
        return controllers;
    }

    @Override
    public void close() {
        controllerProviders.forEach(ControllerProvider::close);
    }
}
