package org.unipop.simpleQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.UniGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = UniGraphGraphProvider.class, graph = UniGraph.class)
public class UniGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}