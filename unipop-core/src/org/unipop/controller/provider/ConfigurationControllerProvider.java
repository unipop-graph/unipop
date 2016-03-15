package org.unipop.controller.provider;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.unipop.structure.UniGraph;

import java.util.List;

public class ConfigurationControllerProvider extends ControllerProvider {

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        HierarchicalConfiguration hierarchicalConfiguration = ConfigurationUtils.convertToHierarchical(configuration);
        List<HierarchicalConfiguration> controllerProviderConfigurations = hierarchicalConfiguration.configurationsAt("mappings");
        if(controllerProviderConfigurations.size() == 0) throw new InstantiationException("No 'mappings' configured for StandardSchemaControllerManager");

        for(HierarchicalConfiguration cpConfiguration : controllerProviderConfigurations) {
            String builderName = cpConfiguration.getString("class");
            ControllerProvider controllerProvider = Class.forName(builderName).asSubclass(ControllerProvider.class).newInstance();
            controllerProvider.init(graph, cpConfiguration);
            controllerProvider.getControllers().forEach(this::addController);
        }
    }
}
