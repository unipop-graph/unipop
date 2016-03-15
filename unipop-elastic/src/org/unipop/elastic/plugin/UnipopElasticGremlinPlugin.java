package org.unipop.elastic.plugin;

import org.apache.tinkerpop.gremlin.groovy.plugin.*;
import org.unipop.elastic.controllerprovider.BasicElasticControllerProvider;
import org.unipop.structure.UniGraph;

import java.util.*;

public class UnipopElasticGremlinPlugin extends AbstractGremlinPlugin {


    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + UniGraph.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + BasicElasticControllerProvider.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "unipop.elastic";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }
}
