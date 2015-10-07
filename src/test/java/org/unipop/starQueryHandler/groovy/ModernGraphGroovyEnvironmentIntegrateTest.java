package org.unipop.starQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.unipop.starQueryHandler.ModernGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = UniGraph.class)
public class ModernGraphGroovyEnvironmentIntegrateTest {
    static {
        SugarLoader.load();
    }
}
