package org.elasticgremlin.process.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.elasticgremlin.ModernStarGraphGraphProvider;
import org.elasticgremlin.testimpl.ModernStarGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ModernStarGraph.class)
public class ModernStarGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
