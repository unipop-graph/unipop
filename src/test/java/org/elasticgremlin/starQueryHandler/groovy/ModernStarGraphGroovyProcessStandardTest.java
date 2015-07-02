package org.elasticgremlin.starQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.elasticgremlin.starQueryHandler.ModernStarGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernStarGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
