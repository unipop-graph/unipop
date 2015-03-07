package com.tinkerpop.gremlin.elastic.groovy.plugin;

import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.groovy.plugin.*;

import java.util.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElasticGraphGremlinPlugin extends AbstractGremlinPlugin {


    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + ElasticGraph.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.elasticgraph";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }
}
