package org.unipop.simpleQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.UniGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = UniGraphGraphProvider.class, graph = UniGraph.class)
public class UniGraphGroovyEnvironmentIntegrateTest {
    static {
        SugarLoader.load();
    }
}