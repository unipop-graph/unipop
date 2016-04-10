package org.unipop.query.controller;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConfigurationControllerManager implements ControllerManager {

    protected Set<SourceProvider> sourceProviders = new HashSet<>();
    protected Set<UniQueryController> controllers = new HashSet<>();

    public ConfigurationControllerManager(UniGraph graph, Configuration configuration) throws Exception {
        HierarchicalConfiguration hierarchicalConfiguration = ConfigurationUtils.convertToHierarchical(configuration);
        List<HierarchicalConfiguration> controllerProviderConfigurations = hierarchicalConfiguration.configurationsAt("source");
        if(controllerProviderConfigurations.size() == 0) throw new InstantiationException("No 'mappings' configured for ConfigurationControllerManager");


        for(HierarchicalConfiguration cpConfiguration : controllerProviderConfigurations) {
            String builderName = cpConfiguration.getString("class");
            SourceProvider sourceProvider = Class.forName(builderName).asSubclass(SourceProvider.class).newInstance();
            sourceProviders.add(sourceProvider);
            sourceProvider.init(graph, cpConfiguration).forEach(this.controllers::add);
        }
    }

    @Override
    public Set<UniQueryController> getControllers() {
        return controllers;
    }

    @Override
    public void close() {
        sourceProviders.forEach(SourceProvider::close);
    }
}
