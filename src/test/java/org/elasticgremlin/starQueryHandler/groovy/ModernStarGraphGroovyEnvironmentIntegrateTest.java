package org.elasticgremlin.starQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.elasticgremlin.starQueryHandler.ModernStarGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernStarGraphGroovyEnvironmentIntegrateTest {
    static {
        SugarLoader.load();
    }
}
