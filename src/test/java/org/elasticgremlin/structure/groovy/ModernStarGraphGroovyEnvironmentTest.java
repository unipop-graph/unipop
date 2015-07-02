package org.elasticgremlin.structure.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.elasticgremlin.ModernStarGraphGraphProvider;
import org.elasticgremlin.testimpl.ModernStarGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ModernStarGraph.class)
public class ModernStarGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}
