package org.unipop.elastic.basic.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.elastic.basic.BasicGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = BasicGraphProvider.class, graph = UniGraph.class)
public class UniGraphGroovyEnvironmentIntegrateTest {
    static {
        SugarLoader.load();
    }
}