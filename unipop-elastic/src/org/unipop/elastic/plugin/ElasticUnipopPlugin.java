package org.unipop.elastic.plugin;

import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.unipop.elastic.ElasticSourceProvider;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

public class ElasticUnipopPlugin extends AbstractGremlinPlugin {


    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + UniGraph.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + ElasticSourceProvider.class.getPackage().getName() + DOT_STAR);
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
