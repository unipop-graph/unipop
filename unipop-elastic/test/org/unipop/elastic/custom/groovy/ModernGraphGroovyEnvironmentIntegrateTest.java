package org.unipop.elastic.custom.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.unipop.elastic.custom.CustomGraphProvider;
import org.junit.runner.RunWith;
import org.unipop.structure.UniGraph;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = UniGraph.class)
public class ModernGraphGroovyEnvironmentIntegrateTest {
    static {
        SugarLoader.load();
    }
}
